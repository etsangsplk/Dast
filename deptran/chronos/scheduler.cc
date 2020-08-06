//
// Created by micha on 2020/3/23.
//


#include "scheduler.h"
#include "commo.h"
#include "deptran/chronos/tx.h"
#include <climits>
#include "deptran/frame.h"
#include <limits>

//Thos XXX_LOG is defined in __dep__.h

using namespace rococo;
class Frame;

chr_ts_t SchedulerChronos::CalculateAnticipatedTs(const chr_ts_t &src_ts, const chr_ts_t &recv_ts) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  chr_ts_t ret;
  int64_t estimated_rtt = (int64_t(recv_ts.timestamp_) - int64_t(src_ts.timestamp_)) * 2;
  Log_info("estimated rtt = %ld, recv_ts = %s, src_ts = %s", estimated_rtt, recv_ts.to_string().c_str(), src_ts.to_string().c_str());
  if  (estimated_rtt < 100 * 1000){
    //< 100ms, TC not added
    estimated_rtt = 200 * 1000;
  }
  ret.timestamp_ =
      recv_ts.timestamp_ + estimated_rtt;
//  ret.timestamp_ =
//      recv_ts.timestamp_ + 2 * (recv_ts.timestamp_ - src_ts.timestamp_) + 50 * 1000; //+100 because no TC added for now
  ret.stretch_counter_ = 0;
  ret.site_id_ = recv_ts.site_id_;

  if (!(ret > max_future_ts_)) {
    ret = max_future_ts_;
    ret.timestamp_++;
  }
  max_future_ts_ = ret;

  return ret;
}

SchedulerChronos::SchedulerChronos(Frame *frame) : Scheduler() {
  this->frame_ = frame;
  auto config = Config::GetConfig();
  for (auto &site: config->SitesByPartitionId(frame->site_info_->partition_id_)) {
    if (site.id != frame->site_info_->id) {
      local_replicas_ts_[site.id] = chr_ts_t();
      local_replicas_clear_ts_[site.id] = chr_ts_t();
      notified_txn_ts[site.id] = chr_ts_t();
      Log_info("created timestamp for local replica for partition %u,  site_id = %hu",
               frame->site_info_->partition_id_,
               site.id);
    }
  }
  local_sync_interval_ms_ = config->chronos_local_sync_interval_ms_;
  verify(!config->replica_groups_.empty());
  //TODO: currently assume the same number of replicas in each shard/replication
  n_replicas = config->replica_groups_[0].replicas.size();

  Log_info("Created scheduler for site %hu, local_sync_interval_ms = %d, n_replicas in each site %d",
           site_id_,
           local_sync_interval_ms_,
           n_replicas);

  if (local_sync_interval_ms_ > 0) {
    local_sync_loop_thread_ = std::thread(&SchedulerChronos::LocalSyncLoop, this);
    local_sync_loop_thread_.detach();
  }

}

void SchedulerChronos::LocalSyncLoop() {
  while (true) {
    SYNC_LOG("going to sync");
    std::unique_lock<std::recursive_mutex> lk(mtx_);
    if (commo_ != nullptr) {
      SYNC_LOG("local_replicas_ts_size = %d", this->local_replicas_ts_.size());
      for (auto &pair: this->local_replicas_ts_){
        siteid_t target_site = pair.first;
        ChronosLocalSyncReq req;
        req.my_clear_ts = my_clear_ts_;
        req.my_clock = GenerateChrTs(true);
        CollectNotifyTxns(target_site, req.piggy_my_pending_txns, req.my_clock);
        auto callback = std::bind(&SchedulerChronos::SyncAck,
                                  this,
                                  target_site,
                                  req.my_clock,
                                  std::placeholders::_1);
        SYNC_LOG("sending sync to site %hu, my clear ts= %s",target_site, my_clear_ts_.to_string().c_str());
        commo()->SendLocalSync(target_site, req, callback);
      }
    }
    lk.unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds{local_sync_interval_ms_});
  }
}

int SchedulerChronos::OnSubmitLocal(const vector<SimpleCommand> &cmd,
                                    const ChronosSubmitReq &chr_req,
                                    ChronosSubmitRes *chr_res,
                                    TxnOutput *output,
                                    const function<void()> &reply_callback) {

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  txnid_t txn_id = cmd[0].root_id_; //should have the same root_id

  //For debug
  Log_info("++++ %s called for txn_id = %lu", __FUNCTION__, txn_id);
  for (auto& c : cmd){
    DEP_LOG("piece is %d, input ready = %d, map size = %d",
        c.inn_id(),
        c.input.piece_input_ready_,
        c.waiting_var_par_map.size());
  }

  //Step 1: push the txns to local_txns_by_me (for later execution) and pending_local_txns

  verify(std::find(local_txns_by_me_.begin(), local_txns_by_me_.end(), txn_id) == local_txns_by_me_.end());
  local_txns_by_me_.push_back(txn_id);
  chr_ts_t ts = GenerateChrTs(true);

  while (pending_txns_.count(ts) != 0) {
    SER_LOG("ts %s with original id = %lu, new id = %lu",
             ts.to_string().c_str(),
             pending_txns_[ts],
             txn_id);
    txnid_t prev_txn_id = pending_txns_[ts]; 
    auto prev_txn = (TxChronos *) (GetOrCreateDTxn(prev_txn_id));
    //This ts may be removed from dist_txn_tss
    if(prev_txn->chr_phase_ != DIST_CAN_EXE){
      Log_warn("here phase = %d", prev_txn->chr_phase_);
      verify(0);
    }
    ts = GenerateChrTs(true);
  } 
    

  Log_info("ts %s set for transaction id = %lu", ts.to_string().c_str(), txn_id);
  pending_txns_[ts] = txn_id;

  /*
  * Step 2: create the txn data structure, and assign the execution callback.
   * The callback does two things
   * a) remote the txn from local_txns_by_me
   * b) reply to client
   * The entry in pending_local_txns will be delted by the CheckExecutable funciton
  */
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  dtxn->chr_phase_ = LOCAL_NOT_STORED;
  dtxn->ts_ = ts;
  dtxn->handler_site_ = site_id_;
  dtxn->region_leader_id_ = site_id_;
  dtxn->pieces_ = cmd;
  dtxn->n_local_pieces_ = cmd.size();
  dtxn->n_executed_pieces = 0;
  dtxn->dist_output_ = output;

  std::function<bool()> execute_callback = [=]() {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    dtxn->RecursiveExecuteReadyPieces(this->partition_id_);

    //this is local txn.
    //Must can finish after one recursive phase
    verify(dtxn->n_local_pieces_ == dtxn->n_executed_pieces);

    //remove the txn from local txns by me (gc)
    auto pos = std::find(local_txns_by_me_.begin(), local_txns_by_me_.end(), txn_id);
    verify(pos != local_txns_by_me_.end());
    local_txns_by_me_.erase(pos);
    verify(std::find(local_txns_by_me_.begin(), local_txns_by_me_.end(), txn_id) == local_txns_by_me_.end());
    Log_info("finished executing transaction (I am leader) for id = %lu, will reply to client, output size = %d",
        txn_id,
        output->size());
    reply_callback();
    return true;
  };

  dtxn->execute_callback_ = execute_callback;
  /*
   * Step 3: assign callback and call the store local rpc.
   */

  verify(dtxn->id() == txn_id);
  verify(cmd[0].partition_id_ == Scheduler::partition_id_);
  verify(cmd[0].root_id_ == txn_id);

  for (auto &pair: this->local_replicas_ts_){
    siteid_t target_site = pair.first;
    ChronosStoreLocalReq req;
    req.txn_ts = ts;
    req.piggy_clear_ts = my_clear_ts_;
    CollectNotifyTxns(target_site, req.piggy_my_pending_txns, req.txn_ts);
    auto ack_callback = std::bind(&SchedulerChronos::StoreLocalAck,
                                  this,
                                  txn_id,
                                  target_site,
                                  req.txn_ts,
                                  std::placeholders::_1);
    commo()->SendStoreLocal(target_site, cmd, req, ack_callback);
  }





  RPC_LOG("----- %s returned for txn_id = %lu", __FUNCTION__, txn_id);
  return 0;
}

int SchedulerChronos::OnSubmitDistributed(const map<parid_t, vector<SimpleCommand>> &cmds_by_par,
                                          const ChronosSubmitReq &chr_req,
                                          ChronosSubmitRes *chr_res,
                                          TxnOutput *output,
                                          const function<void()> &reply_callback) {

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  //This is a distributed txn, and I am the coordinator.
  verify(cmds_by_par.size() > 0);
  verify(cmds_by_par.begin()->second.size() > 0);
  txnid_t txn_id = cmds_by_par.begin()->second.begin()->root_id_; //should have the same root_id


  Log_info("++++ %s called for txn_id = %lu", __FUNCTION__, txn_id);
  /*
   * Step 1:
   * Initialize the status of each region to 0 (not_received) first.
   */
  verify(distributed_txns_by_me_.count(txn_id) == 0);
  distributed_txns_by_me_.insert(txn_id);
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  dtxn->handler_site_ = site_id_;
  dtxn->region_leader_id_ = site_id_;

  /*
   * Step 2: Send the propose request to local replicas and the proxy of each touched region.
   */
#ifdef LAT_BREAKDOWN
  dtxn->handler_submit_ts_ =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif //LAT_BREAKDOWN

  chr_ts_t src_ts = GenerateChrTs(true);

  for (auto &pair: cmds_by_par) {
    //Send to proxy
    parid_t par_id = pair.first;
    if (par_id != partition_id_) {
      //pieces for remote (non-home) partitions
      //Send propose remote to *the proxy* of other replicas.
      auto callback = std::bind(&SchedulerChronos::ProposeRemoteAck,
                                this,
                                txn_id,
                                par_id,
                                std::placeholders::_1,
                                std::placeholders::_2);

      ChronosProposeRemoteReq chr_req;
      chr_req.src_ts = src_ts;
      commo()->SendProposeRemote(pair.second, chr_req, callback);
      dtxn->dist_partition_status_[par_id] = P_REMOTE_SENT;
      dtxn->dist_partition_acks_[par_id] = std::set<uint16_t>();
    } else {
      dtxn->dist_partition_status_[par_id] = P_LOCAL_NOT_SENT;
      dtxn->dist_partition_acks_[par_id] = std::set<uint16_t>();
    }
  }

  dtxn->chr_phase_ = DIST_REMOTE_SENT;



  //XS: no need to care the order of assigning callback and send RPC
  //The scheduler has global lock
  auto remote_prepared_callback = [=]() {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
#ifdef LAT_BREAKDOWN
    dtxn->handler_remote_prepared_ts =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif //LAT_BREAKDOWN

    verify(dtxn->dist_partition_status_[partition_id_] == P_LOCAL_NOT_SENT);
    for (auto &pair: this->local_replicas_ts_){
      auto target_site = pair.first;
      ChronosProposeLocalReq chr_req;
      chr_req.txn_anticipated_ts = dtxn->ts_;
      chr_req.handler_site = site_id_;

      chr_req.piggy_clear_ts = my_clear_ts_;
      chr_req.piggy_my_ts = GenerateChrTs(true);
      CollectNotifyTxns(target_site, chr_req.piggy_my_pending_txns, chr_req.piggy_my_ts);

      auto callback = std::bind(&SchedulerChronos::ProposeLocalACK,
                                this,
                                txn_id,
                                target_site,
                                chr_req.piggy_my_ts,
                                std::placeholders::_1);

      commo()->SendProposeLocal(target_site, cmds_by_par.at(this->partition_id_), chr_req, callback);
    }
    dtxn->dist_partition_status_[partition_id_] = P_LOCAL_SENT;
  };

  dtxn->remote_prepared_callback_ = remote_prepared_callback;
  /*
   * Step 3:
   * Assign the callback for execute
   */


  dtxn->send_output_client = reply_callback;

  dtxn->pieces_ = cmds_by_par.at(this->partition_id_);
  dtxn->n_local_pieces_ = dtxn->pieces_.size();
  dtxn->n_executed_pieces = 0;
  dtxn->dist_output_ = output;

  std::function<bool()> execute_callback = [=]() {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
#ifdef LAT_BREAKDOWN
    int64_t ts_before, ts_after;
    ts_before =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif //LAT_BREAKDOWN
    auto output_to_send = dtxn->RecursiveExecuteReadyPieces(this->partition_id_);
    for (auto& pair :output_to_send){
      parid_t target_par = pair.first;
      if (target_par != this->partition_id_){
        DEP_LOG("Sending output to partition %u, size = %d",
            target_par,
            pair.second.size());
        ChronosSendOutputReq chr_req;
        chr_req.var_values = pair.second;
        chr_req.txn_id = txn_id;
        commo()->SendOutput(target_par, chr_req);
      }
    }

#ifdef LAT_BREAKDOWN
    ts_after =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    Log_info("time for executing txn %lu is %ld us", txn_id, ts_after - ts_before);
#endif //LAT_BREAKDOWN

    bool finished = dtxn->n_local_pieces_ == dtxn->n_executed_pieces;
    DEP_LOG("execution callback, for txn %lu, finished = %d", txn_id, finished);
    if (finished){
#ifdef LAT_BREAKDOWN
      dtxn->handler_local_exe_finish_ts =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif //LAT_BREAKDOWN

      auto pos = std::find(distributed_txns_by_me_.begin(), distributed_txns_by_me_.end(), txn_id);
      verify(pos != distributed_txns_by_me_.end());
      distributed_txns_by_me_.erase(pos);
      SER_LOG("%s called, execution callback", __FUNCTION__);
      dtxn->dist_partition_status_[partition_id_] = P_OUTPUT_RECEIVED;
      dtxn->CheckSendOutputToClient();
    }
    return finished;
  };

  dtxn->execute_callback_ = execute_callback;

  //Step 3: assign callback
  RPC_LOG("----- %s returned for txn_id = %lu", __FUNCTION__, txn_id);
}

void SchedulerChronos::ProposeRemoteAck(txnid_t txn_id,
                                        parid_t par_id,
                                        siteid_t site_id,
                                        ChronosProposeRemoteRes &chr_res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("%s called for txn %lu from par_id %u: site id %hu", __FUNCTION__, txn_id, par_id, site_id);
  //This is the ack from the leader.
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));

  verify(dtxn->dist_partition_acks_.count(par_id) != 0);
  dtxn->dist_partition_acks_[par_id].insert(site_id);
  verify(dtxn->anticipated_ts_.count(par_id) == 0);
  dtxn->anticipated_ts_[par_id] = chr_res.anticipated_ts;
//  if (chr_res.anticipated_ts > dtxn->ts_){
//    dtxn->ts_ = chr_res.anticipated_ts;
//  }
  if (dtxn->dist_partition_acks_[par_id].size() >= n_replicas) {
    verify(dtxn->dist_partition_status_[par_id] == P_REMOTE_SENT);
    dtxn->dist_partition_status_[par_id] = P_REMOTE_PREPARED;
    CheckRemotePrepared(dtxn);
  }
}

void SchedulerChronos::ProposeLocalACK(txnid_t txn_id,
                                       siteid_t target_site,
                                       chr_ts_t told_ts,
                                       ChronosProposeLocalRes &chr_res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("ProposeLocalAck called for txn %lu from site %hu", txn_id, target_site);
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  verify(dtxn->dist_partition_acks_.count(partition_id_) != 0);
  siteid_t src_site = chr_res.my_site_id;
  dtxn->dist_partition_acks_[partition_id_].insert(src_site);
  RPC_LOG("%s called from site %hu", __FUNCTION__, src_site);

  if (this->notified_txn_ts[target_site] < told_ts){
    this->notified_txn_ts[target_site] = told_ts;
    SER_LOG("notified txn ts for site %hu changed to %s",
             target_site,
             told_ts.to_string().c_str());
  }

  if (dtxn->dist_partition_acks_[partition_id_].size() >= n_replicas - 1) {
    verify(dtxn->dist_partition_status_[partition_id_] == P_LOCAL_SENT);
    dtxn->dist_partition_status_[partition_id_] == P_LOCAL_PREPARED;
    verify(dtxn->chr_phase_ == DIST_REMOTE_PREPARED);
    SER_LOG("acked for distributed txn %lu, with ts %s",
             dtxn->id(),
             dtxn->ts_.to_string().c_str());
    verify(dist_txn_tss_.count(dtxn->ts_) != 0);
    dist_txn_tss_.erase(dtxn->ts_);

    dtxn->chr_phase_ = DIST_CAN_EXE;

#ifdef LAT_BREAKDOWN
    dtxn->handler_local_prepared_ts =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif //LAT_BREAKDOWN

    ChronosDistExeReq chr_req;
    chr_req.txn_id = txn_id;
    chr_req.decision_ts = dtxn->ts_;
    for (auto &pair: dtxn->dist_partition_status_) {
      parid_t par_id = pair.first;
        auto callback =
            std::bind(&SchedulerChronos::DistExeAck,
                      this,
                      txn_id,
                      par_id,
                      std::placeholders::_1,
                      std::placeholders::_2);
        commo()->SendDistExe(par_id, chr_req, callback);
    }
    SER_LOG("Move forward");
  }

}

void SchedulerChronos::DistExeAck(txnid_t txn_id, parid_t par_id, TxnOutput &output, ChronosDistExeRes &chr_res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  if (chr_res.is_region_leader == 0){
      return;
  }
  Log_info("%s received from the region leader of partition %u for txn id = %lu", __FUNCTION__, par_id,  txn_id);
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  for (auto &pair: output) {
    SER_LOG("received output for var %u, size = %d", pair.first, pair.second.size());
    verify((*dtxn->dist_output_).count(pair.first) == 0);
    (*dtxn->dist_output_)[pair.first] = pair.second;
  }
  dtxn->dist_partition_status_[par_id] = P_OUTPUT_RECEIVED;
  dtxn->CheckSendOutputToClient();
}

void SchedulerChronos::StoreLocalAck(txnid_t txn_id,
                                     siteid_t target_site,
                                     chr_ts_t told_ts,
                                     ChronosStoreLocalRes &chr_res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("+++ %s called for txn %lu", __FUNCTION__, txn_id);
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  dtxn->n_local_store_acks++;

  SER_LOG("n acks = %d, threshold = %d", dtxn->n_local_store_acks, this->n_replicas -1 );
  if (dtxn->n_local_store_acks == this->n_replicas-1) {
    auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
    dtxn->chr_phase_ = LOCAL_CAN_EXE;
  }

  if (this->notified_txn_ts[target_site] < told_ts){
    this->notified_txn_ts[target_site] = told_ts;
    SER_LOG("notified txn ts for site %hu changed to %s",
             target_site,
             told_ts.to_string().c_str());
  }
//  siteid_t src_site = chr_res.piggy_my_ts.site_id_;
//  chr_ts_t src_ts = chr_res.piggy_my_ts;
//
////  InsertNotifiedTxns(chr_res.piggy_my_pending_txns);
//  UpdateReplicaInfo(src_site, src_ts, chr_res.piggy_clear_ts, chr_res.piggy_my_pending_txns);

  RPC_LOG("--- %s returned", __FUNCTION__);
}

void SchedulerChronos::CheckExecutableTxns() {
  //xs todo: this seems not necessary
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  RPC_LOG("%s called", __FUNCTION__);
  chr_ts_t min_ts = GenerateChrTs(true);
  for (auto &pair: local_replicas_ts_) {
    if (pair.second < min_ts) {
      min_ts = pair.second;
    }
  }

  for (auto itr = pending_txns_.begin(); itr != pending_txns_.end();) {
    Log_info("checking txn %lu with ts %s",
             itr->second,
             itr->first.to_string().c_str());

    if (itr->first > min_ts) {
      Log_info("Not going to execute, queue head ts = %s > min ts = %s",
               itr->first.to_string().c_str(),
               min_ts.to_string().c_str());

      break;
    }
    txnid_t txn_id = itr->second;
    auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));

    if (std::find(local_txns_by_me_.begin(), local_txns_by_me_.end(), txn_id) != local_txns_by_me_.end()) {
      //I am the leader for this transaction.
      if (dtxn->chr_phase_ < PHASE_CAN_EXE) {
        Log_info("Not going to execute local, queue head is my txn id = %lu not ready", itr->second);
        break;
      } else {
        my_clear_ts_ = itr->first;
        if (dtxn->exe_tried_ == false){
          Log_info("[First] Going to execute local txn (I am the leader) with id = %lu=, ts = %s", txn_id,
                   dtxn->ts_.to_string().c_str());
        }
        else{
          Log_info("[Retry] Going to execute local txn (I am the leader) with id = %lu=, ts = %s", txn_id,
                   dtxn->ts_.to_string().c_str());
        }
        bool finished = dtxn->execute_callback_();
        if (finished){
          itr = pending_txns_.erase(itr);
        }else{
          verify(0);
          break;
        }
      }
    } else if (distributed_txns_by_me_.count(txn_id) != 0) {
      if (dtxn->chr_phase_ < PHASE_CAN_EXE) {
        Log_info("Not going to execute dist, queue head is my txn id = %lu not ready to execute", itr->second);
        break;
      } else {
        my_clear_ts_ = itr->first;
        if (dtxn->exe_tried_ == false){
          Log_info("[First] Going to execute dist txn (I am the leader) with id = %lu=, ts = %s", txn_id,
                   dtxn->ts_.to_string().c_str());
        }
        else{
          Log_info("[Retry] Going to execute dist txn (I am the leader) with id = %lu=, ts = %s", txn_id,
                   dtxn->ts_.to_string().c_str());
        }
        bool finished = dtxn->execute_callback_();
        if (finished) {
          itr = pending_txns_.erase(itr);
        } else {
          break;
        }
      }
    } else {
      //I am not the leader for this transaction.
      //TODO: this seems need the water mark from OV.
      //How can I know this transaction can be executed.
      siteid_t src_site = dtxn->region_leader_id_;
      SER_LOG("src site is %hu", src_site);
      verify(local_replicas_clear_ts_.count(src_site) != 0);
      if (!(local_replicas_clear_ts_[src_site] < itr->first) && dtxn->chr_phase_ >= PHASE_CAN_EXE) {
        if (dtxn->exe_tried_ == false){
          Log_info("[First] Going to execute txn (I am not leader) with id = %lu=, ts = %s", txn_id,
                   dtxn->ts_.to_string().c_str());
        }else{
          Log_info("[Retry] Going to execute txn (I am not leader) with id = %lu=, ts = %s", txn_id,
                   dtxn->ts_.to_string().c_str());
        }

        bool finished = dtxn->execute_callback_();
        if (finished){
          itr = pending_txns_.erase(itr);
        }else{
          break;
        }
      } else {
        Log_info("Not going to execute transction %lu, not from me, its leader (%hu) 's clear ts is %s",
                 txn_id,
                 src_site,
                 local_replicas_clear_ts_[src_site].to_string().c_str());
        break;
      }
    }
  }
}

void SchedulerChronos::CheckRemotePrepared(TxChronos *dtxn) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  bool can_move_forward = true;
  for (auto &s: dtxn->dist_partition_status_) {
    if (s.second < P_REMOTE_PREPARED) {
      can_move_forward = false;
      break;
    }
  }
  if (can_move_forward) {
    chr_ts_t max_anticipated_ts;
    for (auto &pair: dtxn->anticipated_ts_) {
      if (pair.second > max_anticipated_ts) {
        max_anticipated_ts = pair.second;
      }
    }
    chr_ts_t my_anticipated;
    do {
      my_anticipated = GenerateChrTs(false);

      if (my_anticipated > max_anticipated_ts){
        max_anticipated_ts = my_anticipated;
      }
      Log_info("My anticipated ts = %s",
               my_anticipated.to_string().c_str());
    } while(dist_txn_tss_.count(max_anticipated_ts) != 0 || pending_txns_.count(max_anticipated_ts) != 0);

    /*
     * This guarantee that there is stretchable space between last and anticipated ts
     */
    dtxn->ts_ = max_anticipated_ts;
    dist_txn_tss_.insert(max_anticipated_ts);

    Log_info("All remote partitions prepared, move forward, txn_id = %lu, anticipated ts = %s", dtxn->id(),
             max_anticipated_ts.to_string().c_str());

    chr_ts_t cur_ts = GenerateChrTs(true);
    if (!(cur_ts < dtxn->ts_)) {
      SER_LOG("cut ts = %s, remote ts = %s",
               cur_ts.to_string().c_str(),
               dtxn->ts_.to_string().c_str());
    }
    verify(cur_ts < dtxn->ts_);
    verify(pending_txns_.count(dtxn->ts_) == 0);
    pending_txns_[dtxn->ts_] = dtxn->id();
    //TODO: Verify that local time is smaller than max_ant_ts
    Log_info("put distributed txn %lu to pending local txn, ts = %s",
             dtxn->id(),
             dtxn->ts_.to_string().c_str());
    verify(dtxn->chr_phase_ == DIST_REMOTE_SENT);
    dtxn->chr_phase_ = DIST_REMOTE_PREPARED;
    //Replicate the remtoe decision, and replicate to local replicas.
    dtxn->remote_prepared_callback_();
    //Send propose local to **all** replicas (encapsulated in the commo send function)
  }
}

void SchedulerChronos::OnProposeRemote(const vector<SimpleCommand> &cmds,
                                       const ChronosProposeRemoteReq &req,
                                       ChronosProposeRemoteRes *chr_res,
                                       const function<void()> &reply_callback) {
  /*
   * Only the leader receives this
   * Step 1: calculate the anticipated ts, put into pending remote txns
   *
   */
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  auto txn_id = cmds[0].root_id_;

  chr_ts_t src_ts = req.src_ts;
  chr_ts_t my_ts = GenerateChrTs(true);


  //I am the region leader of this txn
  verify(distributed_txns_by_me_.count(txn_id) == 0);
  distributed_txns_by_me_.insert(txn_id);

  chr_ts_t anticipated_ts;
  int tmp= 0;
  do{
    src_ts.timestamp_ += tmp++;
    anticipated_ts = CalculateAnticipatedTs(src_ts, my_ts);
  }while(dist_txn_tss_.count(anticipated_ts) != 0 || pending_txns_.count(anticipated_ts) != 0);

  chr_res->anticipated_ts = anticipated_ts;


  Log_info("%s called for txn id = %lu, src_ts = %s, my_ts = %s, anticipated = %s",
           __FUNCTION__,
           cmds[0].root_id_,
           src_ts.to_string().c_str(),
           my_ts.to_string().c_str(),
           anticipated_ts.to_string().c_str());
  /*
   * There should be stretchable space, as the anticipated ts is a future for me
   */
  if (last_clock_ < anticipated_ts){
    verify(dist_txn_tss_.count(anticipated_ts) == 0);
    dist_txn_tss_.insert(anticipated_ts);
  }else{
    verify(0);
  }

  verify(pending_txns_.count(anticipated_ts) == 0);

  this->pending_txns_[anticipated_ts] = txn_id;

  /*
   * Step 2:
   * create the transaction and assign the callback after execution
   *
  */
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  dtxn->ts_ = anticipated_ts;
  dtxn->handler_site_ = src_ts.site_id_;
  dtxn->region_leader_id_ = site_id_;
  dtxn->pieces_ = cmds;
  dtxn->n_local_pieces_ = cmds.size();
  dtxn->n_executed_pieces = 0;
  dtxn->dist_output_ = new TxnOutput;

  std::function<bool()> execute_callback = [=]() {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    Log_info("executing my part of distributed transaction %lu, I am regional leader", txn_id);
    auto output_to_send = dtxn->RecursiveExecuteReadyPieces(this->partition_id_);
    for (auto& pair :output_to_send){
      parid_t target_par = pair.first;
      if (target_par != this->partition_id_){
        DEP_LOG("Sending output to partition %u, size = %d",
                 target_par,
                 pair.second.size());
        ChronosSendOutputReq chr_req;
        chr_req.var_values = pair.second;
        chr_req.txn_id = txn_id;
        commo()->SendOutput(target_par, chr_req);
        }
    }
    bool finished = (dtxn->n_local_pieces_ == dtxn->n_executed_pieces);
    DEP_LOG("execution callback, for txn %lu, finished = %d", txn_id, finished);
    if (finished) {
      dtxn->send_output_to_handler_();
    }
    return finished;
  };
  dtxn->execute_callback_ = execute_callback;



  /*
   * Step3:
   * send to local replicas.
   */

  for (auto &pair: this->local_replicas_ts_){
    siteid_t target_site = pair.first;
    ChronosStoreRemoteReq store_remote_req;
    store_remote_req.handler_site = src_ts.site_id_;
    store_remote_req.region_leader = site_id_;
    store_remote_req.piggy_clear_ts = my_clear_ts_;
    store_remote_req.piggy_my_ts = GenerateChrTs(true);
    store_remote_req.anticipated_ts = anticipated_ts;
    CollectNotifyTxns(target_site, store_remote_req.piggy_my_pending_txns, store_remote_req.piggy_my_ts);


    auto ack_callback = std::bind(&SchedulerChronos::StoreRemoteACK,
                                  this,
                                  txn_id,
                                  target_site,
                                  store_remote_req.piggy_my_ts,
                                  std::placeholders::_1);

    commo()->SendStoreRemote(target_site, cmds, store_remote_req, ack_callback);
  }

  reply_callback();
  RPC_LOG("--On propose Remote returned");
}

void SchedulerChronos::OnProposeLocal(const vector<SimpleCommand> &cmds,
                                      const ChronosProposeLocalReq &req,
                                      ChronosProposeLocalRes *chr_res,
                                      const function<void()> &reply_callback) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("%s called", __FUNCTION__);
  //Should be similar to On Store Local, but this transaction is not saved

  verify(cmds.size() > 0);
  auto txn_id = cmds[0].root_id_;


  //The txn does not have a timestamp for now
  //verify(unassigned_distributed_txns_.count(txn_id) == 0);
  //unassigned_distributed_txns_.insert(txn_id);
  chr_ts_t anticipated_ts = req.txn_anticipated_ts;


  //!!! Seems no need to add to anticipated to ts
  //verify(dist_txn_tss_.count(anticipated_ts) == 0);
  //dist_txn_tss_.insert(anticipated_ts);


  verify(pending_txns_.count(anticipated_ts) == 0);
  pending_txns_[anticipated_ts] = txn_id;

  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  dtxn->ts_ = anticipated_ts;
  dtxn->handler_site_ = req.handler_site;
  dtxn->region_leader_id_ = req.handler_site;
  dtxn->pieces_ = cmds;
  dtxn->n_local_pieces_ = cmds.size();
  dtxn->n_executed_pieces = 0;
  dtxn->dist_output_= new TxnOutput;
  dtxn->own_dist_output_ = true;

  std::function<bool()> execute_callback = [=]() {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    Log_info("executing distributed transaction %lu, I am not the leader", txn_id);
    dtxn->RecursiveExecuteReadyPieces(this->partition_id_);
    //No need to care about output

    bool finished =(dtxn->n_local_pieces_ == dtxn->n_executed_pieces);
    DEP_LOG("execution callback, for txn %lu, finished = %d", txn_id, finished);
    return finished;
  };
  dtxn->execute_callback_ = execute_callback;

  UpdateReplicaInfo(req.handler_site, req.piggy_my_ts, req.piggy_clear_ts, req.piggy_my_pending_txns);

  chr_res->my_site_id = site_id_;
//  chr_res->piggy_clear_ts = my_clear_ts_;
//  chr_res->piggy_my_ts = GenerateChrTs(true);
//  siteid_t src_site = req.handler_site;
//  CollectNotifyTxns(src_site, chr_res->piggy_my_pending_txns, anticipated_ts);

  reply_callback();
  RPC_LOG("%s returned", __FUNCTION__);
}
//Log_info stretchable timestamp implemented here
chr_ts_t SchedulerChronos::GenerateChrTs(bool for_local){
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  chr_ts_t ret;

  //Step 1: generate a monotonic ts first
  auto now = std::chrono::system_clock::now();

  int64_t
      ts = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count() + this->time_drift_ms_;
  ret.timestamp_ = ts;
  ret.site_id_ = site_id_;
  ret.stretch_counter_ = 0;

  if (for_local == false){
      if (ret < last_clock_) {
          ret.timestamp_ = last_clock_.timestamp_ + 1;
      }
      //!! Not setting last.
      //Leave a gap between last and ret;
      return ret;
  }

  //Step 2: Check whether need to stretch
  if (!dist_txn_tss_.empty()) {
    //will check stretch;
    chr_ts_t low_dist_ts = *dist_txn_tss_.begin();
    Log_info("ret = %s, last = %s, low = %s",
              ret.to_string().c_str(),
              last_clock_.to_string().c_str(),
              low_dist_ts.to_string().c_str());

    if (!(ret < low_dist_ts)) {
      verify(last_clock_ < low_dist_ts);
      ret.timestamp_ = last_clock_.timestamp_;
      ret.stretch_counter_ = last_clock_.stretch_counter_ + 1;
      verify(ret < low_dist_ts);
    } else {
      if (ret < last_clock_) {
        ret.timestamp_ = last_clock_.timestamp_ + 1;
      }
    }
  } else {
    if (ret < last_clock_) {
      ret.timestamp_ = last_clock_.timestamp_ + 1;
    }
  }
  //Log_info("last clock changed to %lu:%lu:%hu",
  //    ret.timestamp_,
  //    ret.stretch_counter_,
  //  ret.site_id_);
  last_clock_ = ret;
  return ret;
}

void SchedulerChronos::OnRemotePrepared(const ChronosRemotePreparedReq &req,
                                        ChronosRemotePreparedRes *chr_res,
                                        const function<void()> &callback) {

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("%s called, for txn id = %lu from site %hu", __FUNCTION__, req.txn_id, req.my_site_id);

  parid_t par_id = req.my_partition_id;
  siteid_t site_id = req.my_site_id;
  txnid_t txn_id = req.txn_id;

  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));

  verify(dtxn->dist_partition_acks_.count(par_id) != 0);
  dtxn->dist_partition_acks_[par_id].insert(site_id);

  if (dtxn->dist_partition_acks_[par_id].size() >= n_replicas) {
    verify(dtxn->dist_partition_status_[par_id] == P_REMOTE_SENT);
    dtxn->dist_partition_status_[par_id] = P_REMOTE_PREPARED;
    CheckRemotePrepared(dtxn);
  }
  callback();
  RPC_LOG("%s returned, for txn id = %lu from site %hu", __FUNCTION__, req.txn_id, req.my_site_id);
}

void SchedulerChronos::OnStoreLocal(const vector<SimpleCommand> &cmd,
                                    const ChronosStoreLocalReq &chr_req,
                                    ChronosStoreLocalRes *chr_res,
                                    const function<void()> &reply_callback) {

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("++ %s called, txnid = %lu, ts = %s", __FUNCTION__, cmd[0].root_id_,
           chr_req.txn_ts.to_string().c_str());

  chr_ts_t ts = chr_req.txn_ts;
  siteid_t src_site = ts.site_id_;

  chr_ts_t clear_ts = chr_req.piggy_clear_ts;

  auto txn_id = cmd[0].root_id_;

  if (this->pending_txns_.count(ts) != 0) {
    Log_info("received tx with same ts %s, id in map = %lu, received id = %lu",
             ts.to_string().c_str(),
             pending_txns_[ts],
             txn_id);
    verify(pending_txns_[ts] == txn_id);
  }

  this->pending_txns_[ts] = txn_id;
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  dtxn->ts_ = ts;
  dtxn->handler_site_ = ts.site_id_;
  dtxn->region_leader_id_ = ts.site_id_;
  dtxn->chr_phase_ = LOCAL_CAN_EXE;
  dtxn->pieces_ = cmd;
  dtxn->n_local_pieces_ = cmd.size();
  dtxn->n_executed_pieces = 0;
  dtxn->dist_output_ = new TxnOutput;
  dtxn->own_dist_output_ = true;

  std::function<bool()> execute_callback = [=]() {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    Log_info("executing local transaction %lu, I am not the leader", txn_id);
    dtxn->RecursiveExecuteReadyPieces(this->partition_id_);
    //should always return true;
    verify(dtxn->n_local_pieces_ == dtxn->n_executed_pieces);
    return true;
  };
  dtxn->execute_callback_ = execute_callback;
  dtxn->region_leader_id_ = chr_req.txn_ts.site_id_;
  dtxn->handler_site_ = chr_req.txn_ts.site_id_;

  auto my_ts = GenerateChrTs(true);
//  chr_res->piggy_my_ts = my_ts;
//  chr_res->piggy_clear_ts = my_clear_ts_;
  //My Pending transaction should be inserted in ts order.

  UpdateReplicaInfo(ts.site_id_, ts, clear_ts, chr_req.piggy_my_pending_txns);
//  CollectNotifyTxns(src_site, chr_res->piggy_my_pending_txns, ts);

//  Log_info("chr_res->my_ts = %lu:%lu:%hu, chr_res->clear_ts = %lu:%lu:%hu, res = %d",
//           chr_res->piggy_my_ts.timestamp_,
//           chr_res->piggy_my_ts.stretch_counter_,
//           chr_res->piggy_my_ts.site_id_,
//           chr_res->piggy_clear_ts.timestamp_,
//           chr_res->piggy_clear_ts.stretch_counter_,
//           chr_res->piggy_clear_ts.site_id_,
//           *res);

  reply_callback();
  RPC_LOG("-- %s returned", __FUNCTION__);
}

void SchedulerChronos::OnDistExe(const ChronosDistExeReq &chr_req,
                                   ChronosDistExeRes *chr_res,
                                   TxnOutput *output,
                                   const function<void()> &reply_callback) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);

  txnid_t txn_id = chr_req.txn_id;
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));

  Log_info("%s called for txn %lu, handler_site = %hu", __FUNCTION__, txn_id, dtxn->handler_site_);
  if (this->local_replicas_ts_.count(dtxn->handler_site_) == 0) {
    //This (previous line) ``if'' is not needed.
    //Keep for readbility.
    //The nodes in the same region as the handler did not add ts to dist_txn_tss
    //
    if (dist_txn_tss_.count(dtxn->ts_) != 0) {
      dist_txn_tss_.erase(dtxn->ts_);
      Log_info("removing ts from dist_txn_tss %s",
               dtxn->ts_.to_string().c_str());
    } else {
      Log_info("not removing ts from dist_txn_tss %s, it was not added ",
               dtxn->ts_.to_string().c_str());
    }
  }


  dtxn->chr_phase_ = DIST_CAN_EXE;
  
  chr_ts_t original_ts = dtxn->ts_;
  verify(pending_txns_.count(original_ts) != 0);
  pending_txns_.erase(original_ts);
  chr_ts_t new_ts = chr_req.decision_ts;

  verify(pending_txns_.count(new_ts) == 0);
  pending_txns_[new_ts] = txn_id;

  dtxn->ts_ = chr_req.decision_ts;


  if (distributed_txns_by_me_.count(txn_id) != 0){
    chr_res->is_region_leader = 1; 
    Log_info("%s called for id = %lu, I am regional leader, phase = %d", __FUNCTION__, txn_id, dtxn->chr_phase_);
    dtxn->send_output_to_handler_ = [=]() {
      std::lock_guard<std::recursive_mutex> guard(mtx_);
      Log_info("Sending execution output to the handler");
      *output = *dtxn->dist_output_; //Copy the output
      reply_callback();
    };
  }else{
    chr_res->is_region_leader = 0; 
    Log_info("%s called for id = %lu, I am not regional leader", __FUNCTION__, txn_id);
    reply_callback();
  }



  CheckExecutableTxns();
}

void SchedulerChronos::OnSendOutput(const ChronosSendOutputReq& chr_req,
                  ChronosSendOutputRes *chr_res,
                  const function<void()>& reply_callback){

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("%s called, received output for txn %lu", __FUNCTION__, chr_req.txn_id);
  auto txn_id = chr_req.txn_id;
  auto dtxn = (TxChronos *) (GetDTxn(txn_id));
  verify(dtxn != nullptr);

  bool more_to_run = dtxn->MergeCheckReadyPieces(chr_req.var_values);
  if (more_to_run){
    if (dtxn->n_executed_pieces > 0){
      Log_info("txn %lu is executing, executing pieces", txn_id);
      bool finished = dtxn->execute_callback_();
      if (finished){
        verify(this->pending_txns_.count(dtxn->ts_) != 0);
        verify (this->pending_txns_[dtxn->ts_] == txn_id);
        this->pending_txns_.erase(dtxn->ts_);
      }else{
        Log_info("n_executed = %d, n_total = %d", dtxn->n_executed_pieces, dtxn->n_local_pieces_);
      }
    }else{
      Log_info("txn %lu is still waiting, not executing pieces", txn_id);
    }
  }else{
    Log_info("txn %lu's input is not ready yet", txn_id);
  }
  reply_callback();
}
void SchedulerChronos::CollectNotifyTxns(uint16_t site,
                                         std::vector<ChrTxnInfo> &into_vecotr,
                                         chr_ts_t up_to_ts) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);

  for (auto &tid : local_txns_by_me_) {
    auto p_txn = (TxChronos *)(GetDTxn(tid));
    verify(p_txn != nullptr);
    if (p_txn->ts_ < notified_txn_ts[site]) {
      continue;
    }
    if (p_txn->ts_ > up_to_ts) {
      verify(0); //this should not happen, a future txn?
      break;
    }

    ChrTxnInfo info;
    info.txn_id = tid, info.ts = p_txn->ts_;
    info.region_leader = p_txn->region_leader_id_;
    into_vecotr.push_back(info);
    SER_LOG(
        "notifying site %hu the existence of txn %lu, with regional leader %hu, with ts %s",
        site,
        tid,
        p_txn->region_leader_id_,
        p_txn->ts_.to_string().c_str());
  }
}

void SchedulerChronos::OnStoreRemote(const vector<SimpleCommand> &cmd,
                                     const ChronosStoreRemoteReq &chr_req,
                                     ChronosStoreRemoteRes *chr_res,
                                     const function<void()> &reply_callback) {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("%s called for txn id = %lu, ts = %s",
           __FUNCTION__,
           cmd[0].root_id_,
           chr_req.anticipated_ts.to_string().c_str());

  auto txn_id = cmd[0].root_id_;

  chr_ts_t anticipated_ts = chr_req.anticipated_ts;


  if (last_clock_ < anticipated_ts){
    /*
     * Important, If we still can stretch
     */
    verify(dist_txn_tss_.count(anticipated_ts) == 0);
    dist_txn_tss_.insert(anticipated_ts);
    Log_info("txn id = %d, There is still stretchable space between last %s and anticipated ts %s",
        txn_id,
        last_clock_.to_string().c_str(),
        anticipated_ts.to_string().c_str());
  }else{
    Log_info("txn id = %d, last %s already larger than ts %s, no stretchable sapce",
             txn_id,
             last_clock_.to_string().c_str(),
             anticipated_ts.to_string().c_str());
  }

  verify(pending_txns_.count(anticipated_ts) == 0);
  this->pending_txns_[anticipated_ts] = txn_id;

  /*
   * Step 2:
   * create the transaction and assign the callback after execution
   *
  */
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  dtxn->ts_ = anticipated_ts;
  dtxn->handler_site_ = chr_req.handler_site;
  dtxn->region_leader_id_ = chr_req.region_leader;
  dtxn->pieces_ = cmd;
  dtxn->n_local_pieces_ = cmd.size();
  dtxn->n_executed_pieces = 0;
  dtxn->dist_output_ = new TxnOutput;
  dtxn->own_dist_output_ = true;


  std::function<bool()> execute_callback = [=]() {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    Log_info("executing remote transaction %lu, I am not the leader", txn_id);
    //No need to send output to others, let the regional leader do that.
    dtxn->RecursiveExecuteReadyPieces(this->partition_id_);
    bool finished = (dtxn->n_local_pieces_ == dtxn->n_executed_pieces);
    Log_info("execution callback, for txn %lu, finished = %d", txn_id, finished);
    return finished;
  };
  dtxn->execute_callback_ = execute_callback;

  ChronosRemotePreparedReq ack_to_handler;
  ack_to_handler.txn_id = txn_id;
  ack_to_handler.my_site_id = this->site_id_;
  ack_to_handler.my_partition_id = this->partition_id_;

  UpdateReplicaInfo(chr_req.region_leader, chr_req.piggy_my_ts, chr_req.piggy_clear_ts, chr_req.piggy_my_pending_txns);
//  chr_res->piggy_my_ts = GenerateChrTs(true);
//  CollectNotifyTxns(chr_req.region_leader, chr_res->piggy_my_pending_txns, chr_req.piggy_my_ts);

  siteid_t hanlder_site = chr_req.handler_site;
  commo()->SendRemotePrepared(hanlder_site, ack_to_handler);

  reply_callback();
  RPC_LOG("%s returned", __FUNCTION__);
}

void SchedulerChronos::StoreRemoteACK(txnid_t txn_id,
                                      siteid_t target_site,
                                      chr_ts_t told_ts,
                                      ChronosStoreRemoteRes &chr_res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  Log_info("%s called", __FUNCTION__);
  auto dtxn = (TxChronos *) (GetOrCreateDTxn(txn_id));
  //No need to do anything, except for update using piggy back information,

//  siteid_t src_site = chr_res.piggy_my_ts.site_id_;
////  InsertNotifiedTxns(chr_res.piggy_my_pending_txns);
//  UpdateReplicaInfo(src_site, chr_res.piggy_my_ts, chr_res.piggy_clear_ts, chr_res.piggy_my_pending_txns);

  if (this->notified_txn_ts[target_site] < told_ts){
    this->notified_txn_ts[target_site] = told_ts;
    SYNC_LOG("notified txn ts for site %hu changed to %s",
             target_site,
             told_ts.to_string().c_str());
  }
  RPC_LOG("--- %s returned", __FUNCTION__);
}

void SchedulerChronos::UpdateReplicaInfo(uint16_t for_site_id, const chr_ts_t &ts, const chr_ts_t &clear_ts,
                                         const std::vector<ChrTxnInfo>& piggy_pending_txns) {
  /*
   * Important: Note that NOT necessarily site_id == ts.site_id_ (for distributed)
  */
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  bool need_check = false;
  verify(local_replicas_clear_ts_.count(for_site_id) != 0);
  verify(local_replicas_ts_.count(for_site_id) != 0);
  if (local_replicas_ts_[for_site_id] < ts) {
    local_replicas_ts_[for_site_id] = ts;
    SYNC_LOG("time clock for site %hu changed to %s",
              for_site_id,
              ts.to_string().c_str());
    need_check = true;
  }

  if (local_replicas_clear_ts_[for_site_id] < clear_ts) {
    local_replicas_clear_ts_[for_site_id] = clear_ts;
    SYNC_LOG("clear timestamp for site %hu changed to %s",
              for_site_id,
              clear_ts.to_string().c_str());
    need_check = true;
  }

  for (auto &info: piggy_pending_txns) {
    //pair <txnid, ts>
    txnid_t tid = info.txn_id;
    chr_ts_t tts = info.ts;
    //use this to determine whether the transaction is known
    //rather than using pending_txns (this txn may already been executed)
    auto dtxn = (TxChronos *) (GetDTxn(tid));
    if (dtxn != nullptr){
      if (pending_txns_.count(tts) == 0) {
        //Have already been removed from pending
        verify(dtxn->chr_phase_ >= PHASE_CAN_EXE);
      } else {
        verify(pending_txns_[tts] == tid);
        Log_debug("already know the existence of transaction id = %lu, with ts %s",
                 tid,
                 tts.to_string().c_str());
      }
    }else{
      Log_debug("Notified with the existence of transaction id = %lu, with ts %s, region_leader = %hu",
               tid,
               tts.to_string().c_str(),
               info.region_leader);
      dtxn = (TxChronos* ) (CreateDTxn(tid));
      pending_txns_[tts] = tid;
      dtxn->region_leader_id_ = info.region_leader;
    }
  }

  if (need_check) {
    CheckExecutableTxns();
  }
}

void SchedulerChronos::OnSync(const ChronosLocalSyncReq &req,
                              ChronosLocalSyncRes *res,
                              const function<void()> &reply_callback) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  SYNC_LOG("***+++ %s called form site = %hu", __FUNCTION__, req.my_clock.site_id_);

  UpdateReplicaInfo(req.my_clock.site_id_, req.my_clock, req.my_clear_ts, req.piggy_my_pending_txns);

  reply_callback();
  SYNC_LOG("***--- %s returned", __FUNCTION__);
}

void SchedulerChronos::SyncAck(siteid_t from_site,
                               chr_ts_t told_ts,
                               ChronosLocalSyncRes &res) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  SYNC_LOG("++++++ %s called from site %hu", __FUNCTION__, from_site);
//  UpdateReplicaInfo(res.ret_clock.site_id_, res.ret_clock, res.ret_clear_ts);
  if (this->notified_txn_ts[from_site] < told_ts){
    this->notified_txn_ts[from_site] = told_ts;
    SYNC_LOG("notified txn ts for site %hu changed to %s",
        from_site,
        told_ts.to_string().c_str());
  }

  SYNC_LOG("------ %s returned", __FUNCTION__);
}

ChronosCommo *SchedulerChronos::commo() {

  auto commo = dynamic_cast<ChronosCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}


