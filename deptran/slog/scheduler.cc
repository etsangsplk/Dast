//
// Created by micha on 2020/3/23.
//


#include "deptran/slog/scheduler.h"
#include "deptran/slog/commo.h"
#include "deptran/slog/tx.h"
#include <climits>
#include "deptran/frame.h"
#include <limits>

//Thos XXX_LOG is defined in __dep__.h

using namespace rococo;
class Frame;

SchedulerSlog::SchedulerSlog(Frame *frame) : Scheduler() {
  this->frame_ = frame;
  auto config = Config::GetConfig();
  this->site_id_ = frame->site_info_->id;
  verify(!config->replica_groups_.empty());
  //TODO: currently assume the same number of replicas in each shard/replication
  n_replicas_ = config->replica_groups_[0].replicas.size();

  Log_info("Created scheduler for site %hu, sync_interval_ms = %d, n_replicas in each site %d",
           site_id_,
           0,
           n_replicas_);


  for (auto site: config->SitesByPartitionId(frame->site_info_->partition_id_)) {
    if (site.id != this->site_id_){
      //TODO: actually only the leader has follower sites.
      //For other sites, this should be empty
      follower_sites_.insert(site.id);
      Log_info("added follower site %hu, my site id = %hu", site.id, this->site_id_);
    }
  }

  check_batch_loop_thread = std::thread(&SchedulerSlog::CheckBatchLoop, this);
  check_batch_loop_thread.detach();

}

int SchedulerSlog::OnSubmitLocal(const vector<SimpleCommand> &cmd,
                                    TxnOutput *output,
                                    const function<void()> &reply_callback) {

  //In slog, the leader is always the leader
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



  //Push to local log
  auto dtxn = (TxSlog *) (GetOrCreateDTxn(txn_id));
  dtxn->is_local_ = true;
  txn_log_.push_back(dtxn);
  uint64_t local_index = txn_log_.size() - 1;

  Log_info("local txn id = %lu has been inserted to the %lu th of the log",
               txn_id,
               local_index);

  //assign execute callback

  dtxn->pieces_ = cmd;
  dtxn->n_local_pieces_ = cmd.size();
  dtxn->n_executed_pieces = 0;
  dtxn->dist_output_ = output;
  id_to_index_map_[txn_id] = local_index;

  std::function<bool()> execute_callback = [=]() {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    dtxn->RecursiveExecuteReadyPieces(this->partition_id_);
    InsertBatch(dtxn);

    //this is local txn.
    //Must can finish after one recursive phase
    verify(dtxn->n_local_pieces_ == dtxn->n_executed_pieces);

    //remove the txn from local txns by me (gc)
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

  //Replicate the log

  auto ack_callback = std::bind(&SchedulerSlog::ReplicateLogLocalAck, this, local_index);
  for (auto siteid :  follower_sites_){
    commo() -> SendReplicateLocal(siteid, cmd, local_index, this->local_log_commited_index_, ack_callback);
  }

  RPC_LOG("----- %s returned for txn_id = %lu", __FUNCTION__, txn_id);
  return 0;
}

void SchedulerSlog::ReplicateLogLocalAck(uint64_t local_index) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  auto dtxn = this->txn_log_[local_index];
  dtxn->n_local_store_acks++;
  SER_LOG("n acks = %d, threshold = %d, id = %lu", dtxn->n_local_store_acks, this->n_replicas_ - 1, dtxn->id());
  if (dtxn->n_local_store_acks == this->n_replicas_ - 1) {
    if (dtxn->is_local_) {
      dtxn->slog_phase_ = TxSlog::SLOG_LOCAL_REPLICATED;
    } else {
      dtxn->slog_phase_ = TxSlog::SLOG_DIST_REPLICATED;
    }
  }
  //Check whether there are node to commit
  Log_info("local commited index =%lu", local_log_commited_index_);
  auto itr = txn_log_.begin();
  itr = itr + local_log_commited_index_ + 1;

  while (itr != txn_log_.end()) {
    auto txn_to_exe = *itr;
    if ((*itr)->slog_phase_ == TxSlog::SLOG_LOCAL_REPLICATED) {
      Log_info("committing index %lu", itr - txn_log_.begin());
      local_log_commited_index_++;
      verify(local_log_commited_index_ == itr - txn_log_.begin());
      Log_info("[First] Going to execute transaction, I am the leader, id = %lu", (*itr)->id());
      (*itr)->execute_callback_();
      Log_info("finish executing transaction, I am the leader, id = %lu", (*itr)->id());
      itr++;
      continue;
    } else if (txn_to_exe->slog_phase_ >= TxSlog::SLOG_DIST_REPLICATED) {
      Log_info("Trying to commit index %lu (distributed), id =%lu, phase =%d",
               itr - txn_log_.begin(),
               (*itr)->id(),
               (*itr)->slog_phase_);
      if (txn_to_exe->slog_phase_ == TxSlog::SLOG_DIST_REPLICATED) {
        txn_to_exe->slog_phase_ = TxSlog::SLOG_DIST_CAN_EXE;
        Log_info("[First] Going to execute transaction, I am the leader, id = %lu", (*itr)->id());
        txn_to_exe->execute_callback_();
        txn_to_exe->par_output_received_[partition_id_] = true;
        //xs TODO: this is magical
        bool should_finish = true;
        for (auto &ready : txn_to_exe->par_output_received_) {
          if (ready.second == false) {
            should_finish = false;
            Log_info("Should not finish, output for par %u not received", ready.first);
          }
        }
        if (should_finish) {
          Log_info("txn id = %lu Should finish", txn_to_exe->id());
          txn_to_exe->slog_phase_ = TxSlog::SLOG_DIST_OUTPUT_READY;
          txn_to_exe->CheckSendOutputToClient();
          local_log_commited_index_++;
          verify(local_log_commited_index_ == itr - txn_log_.begin());
          if (txn_to_exe->slog_phase_ < TxSlog::SLOG_DIST_EXECUTED) {
            Log_info("Here phase = %d", txn_to_exe->slog_phase_);
            verify(txn_to_exe->slog_phase_ >= TxSlog::SLOG_DIST_EXECUTED);
          }
          Log_info("Committed index %lu (distributed)", itr - txn_log_.begin());
          itr++;
        } else {
          Log_info("execution of txn %lu not finished", (*itr)->id());
          break;
        }
      } else if (txn_to_exe->slog_phase_ >= TxSlog::SLOG_DIST_OUTPUT_READY) {
        local_log_commited_index_++;
        verify(local_log_commited_index_ == itr - txn_log_.begin());
        Log_info("Committed index %lu (distributed), phase = %d", itr - txn_log_.begin(), txn_to_exe->slog_phase_);
        itr++;
      } else {
        Log_info("execution of txn %lu not finished", (*itr)->id());
        break;
      }
    } else {
      break;
    }
  }
RPC_LOG("--- %s returned", __FUNCTION__);
}

int SchedulerSlog::OnInsertDistributed(const vector<SimpleCommand> &cmd,
                                       uint64_t global_index,
                                       const std::vector<parid_t> &touched_pars,
                                       siteid_t handler_site) {

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  //This is a distributed txn, and I am the coordinator.
  auto g_log_len = global_log_.size();

  auto c = std::make_unique<std::vector<SimpleCommand>>();
  *c = cmd;

  if (global_index < g_log_len){
    verify(global_log_[global_index] == nullptr);
    global_log_[global_index] = std::move(c);
  }
  else if (global_index == g_log_len){
    global_log_.push_back(std::move(c));
  }else{
    while(global_log_.size() < global_index){
      Log_info("Global log skipping %lu, not received yet", global_log_.size()-1);
      global_log_.push_back(nullptr);
    }
    global_log_.push_back(std::move(c));
  }


  while(global_log_next_index_ < global_log_.size()){
    if (global_log_[global_log_next_index_] == nullptr){
      Log_info("Not Processing index %lu of global log for now, not received yet", global_log_next_index_);
      break;
    }
    Log_info("Processing index %lu of global log", global_log_next_index_);

    const vector<SimpleCommand>& cmds = *(global_log_[global_log_next_index_]);

    if (cmds.size() == 0){
      Log_info("processing global log, skipping global log index %lu", global_index);
      global_log_next_index_++;
      continue;
    }
    txnid_t txn_id = cmds[0].root_id_;
    Log_info("processing global log txn id %lu at global index %lu",
             txn_id,
             global_index);


    if (GetDTxn(txn_id) == nullptr){
      Log_info("this is a new txn");
    }
    auto dtxn = (TxSlog *) (GetOrCreateDTxn(txn_id));
    dtxn->is_local_ = false;
    txn_log_.push_back(dtxn);
    uint64_t local_index = txn_log_.size() - 1;

    id_to_index_map_[txn_id] = local_index;

    dtxn->pieces_ = cmds;
    dtxn->n_local_pieces_ = dtxn->pieces_.size();
    dtxn->n_executed_pieces = 0;
    bool is_handler = true;
    if (dtxn->handler_site_ != this->site_id_){
      Log_info("I am not handler for txn id %lu", txn_id);
      is_handler = false;
      dtxn->dist_output_ = new TxnOutput;
      dtxn->send_output_client = [](){
        Log_info("no need to send output to client");
      };
    }

    std::function<bool()> execute_callback = [=]() {
      std::lock_guard<std::recursive_mutex> guard(mtx_);
      dtxn->RecursiveExecuteReadyPieces(this->partition_id_);
      InsertBatch(dtxn);
      Log_info("output for par %u of txn %lu turned to true", partition_id_, txn_id);
      dtxn->slog_phase_ = TxSlog::SLOG_DIST_EXECUTED;
      if (is_handler){
       dtxn->CheckSendOutputToClient();
      }
      return true;
    };

    dtxn->execute_callback_ = execute_callback;
    verify(dtxn->id() == txn_id);
    verify(cmds[0].root_id_ == txn_id);

    //Replicate the log

    auto ack_callback = std::bind(&SchedulerSlog::ReplicateLogLocalAck, this, local_index);
    for (auto siteid :  follower_sites_){
      commo() -> SendReplicateLocal(siteid, cmds, local_index, this->local_log_commited_index_, ack_callback);
    }

    RPC_LOG("----- %s returned for txn_id = %lu", __FUNCTION__, txn_id);
    global_log_next_index_ ++;
  }
  return 0;
}







void SchedulerSlog::OnSendOutput(const ChronosSendOutputReq& chr_req,
                  ChronosSendOutputRes *chr_res,
                  const function<void()>& reply_callback){

//  std::lock_guard<std::recursive_mutex> guard(mtx_);
//  Log_info("%s called, received output for txn %lu", __FUNCTION__, chr_req.txn_id);
//  auto txn_id = chr_req.txn_id;
//  auto dtxn = (TxSlog *) (GetDTxn(txn_id));
//  verify(dtxn != nullptr);
//
//  bool more_to_run = dtxn->MergeCheckReadyPieces(chr_req.var_values);
//  if (more_to_run){
//    if (dtxn->n_executed_pieces > 0){
//      Log_info("txn %lu is executing, executing pieces", txn_id);
//      bool finished = dtxn->execute_callback_();
//      if (finished){
//        verify(this->pending_txns_.count(dtxn->ts_) != 0);
//        verify (this->pending_txns_[dtxn->ts_] == txn_id);
//        this->pending_txns_.erase(dtxn->ts_);
//      }else{
//        Log_info("n_executed = %d, n_total = %d", dtxn->n_executed_pieces, dtxn->n_local_pieces_);
//      }
//    }else{
//      Log_info("txn %lu is still waiting, not executing pieces", txn_id);
//    }
//  }else{
//    Log_info("txn %lu's input is not ready yet", txn_id);
//  }
  reply_callback();
}



SlogCommo *SchedulerSlog::commo() {

  auto commo = dynamic_cast<SlogCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}


void SchedulerSlog::OnReplicateLocal(const vector<SimpleCommand> &cmd,
                      uint64_t index,
                      uint64_t commit_index,
                      const function<void()> &callback){

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


  //Push to local log
  auto dtxn = (TxSlog *) (GetOrCreateDTxn(txn_id));

  auto log_len = txn_log_.size();

  //This code can be simplified. But the current version is easier to understand
  if (index < log_len){
    //This txn is late
    verify(txn_log_[index] == nullptr);
    txn_log_[index] = dtxn;
  }
  else if (index == log_len){
    txn_log_.push_back(dtxn);
  }
  else {
    //Skip the middle
    while(txn_log_.size() < index){
      txn_log_.push_back(nullptr);
    }
    txn_log_.push_back(dtxn);
  }

  id_to_index_map_[txn_id] = index;


  Log_info("local txn id = %lu has been inserted to the %lu th of log by leader, commit index = %lu",
           txn_id,
           index,
           commit_index);

  //assign execute callback

  dtxn->pieces_ = cmd;
  dtxn->n_local_pieces_ = cmd.size();
  dtxn->n_executed_pieces = 0;
  dtxn->dist_output_ = new TxnOutput;

  std::function<bool()> execute_callback = [=]() {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    dtxn->RecursiveExecuteReadyPieces(this->partition_id_);

    //this is local txn.
    //Must can finish after one recursive phase
    //    verify(dtxn->n_local_pieces_ == dtxn->n_executed_pieces);

    Log_info("finished executing transaction index = %lu (I am not leader) for id = %lu, will reply to client",
             index,
             txn_id);
    return true;
  };

  dtxn->execute_callback_ = execute_callback;

  if (commit_index > local_log_commited_index_ ||  local_log_commited_index_ == -1){
    local_log_commited_index_ = commit_index;
    Log_info("commit index updated to %lu", commit_index);
  }
  for (uint64_t cur = backup_executed_index_ + 1; cur < txn_log_.size(); cur++){
    if (cur > local_log_commited_index_){
      break;
    }
    if (txn_log_[cur] != nullptr){
      txn_log_[cur]->execute_callback_();
      Log_info("[First] Going to execute txn (I am not leader) for txn id = %lu", txn_log_[cur]->id());
      backup_executed_index_ = cur;
    }
  }


  verify(dtxn->id() == txn_id);
  verify(cmd[0].partition_id_ == Scheduler::partition_id_);
  verify(cmd[0].root_id_ == txn_id);

  //Replicate the log

  RPC_LOG("----- %s returned for txn_id = %lu", __FUNCTION__, txn_id);
  callback();
}

void SchedulerSlog::OnSubmitDistributed(const map<parid_t, vector<SimpleCommand>> &cmds_by_par,
                                        TxnOutput *output,
                                        const function<void()> &reply_callback) {
  //Create the txn meta data
  std::lock_guard<std::recursive_mutex> guard(mtx_);

  verify(cmds_by_par.size() > 0);
  verify(cmds_by_par.begin()->second.size() > 0);
  txnid_t txn_id = cmds_by_par.begin()->second.begin()->root_id_; //should have the same root_id

  Log_info("%s called for txn %lu", __FUNCTION__, txn_id);

  auto dtxn = (TxSlog *) (GetOrCreateDTxn(txn_id));

  dtxn->dist_output_ = output;
  dtxn->send_output_client = [=](){
    Log_info("Sending output to client");
    reply_callback();
  };
  dtxn->handler_site_ = this->site_id_;
  for (auto& pair: cmds_by_par){
     dtxn->par_output_received_[pair.first] = false;
  }
  //Submit to raft;
  commo()->SendToRaft(cmds_by_par, this->site_id_);
}
void SchedulerSlog::OnSendBatchRemote(uint32_t src_par, const std::vector<std::pair<txnid_t, TxnOutput>> &batch) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  //
  Log_info("%s called from stc_par %u", __FUNCTION__, src_par);
  for (auto &pair: batch) {
    auto txn_id = pair.first;

    auto dtxn = (TxSlog *) GetDTxn(txn_id);
    if (dtxn == nullptr) {
      verify(id_to_index_map_.count(txn_id) == 0);
      Log_info("Received log for txn id %lu, not related for me", txn_id);
      continue;
    } else {
      //This should be a distributed txn, and I am part of it.
      if (id_to_index_map_.count(txn_id) != 0) {
        auto index = id_to_index_map_[txn_id];
        Log_info("Received log for txn id %lu, at index %lu", txn_id, index);
      } else {
        Log_info("Received log for txn id %lu, raft not inserted yet", txn_id);
      }

      dtxn->dist_output_->insert(pair.second.begin(), pair.second.end());
      Log_info("output for txn %lu received, status for par %hu changed to true, phase = %d", dtxn->id(), src_par, dtxn->slog_phase_);
      dtxn->par_output_received_[src_par] = true;
      if (dtxn->slog_phase_ >= TxSlog::SLOG_DIST_CAN_EXE) {
        //xs todo: this is magical
        dtxn->RecursiveExecuteReadyPieces(partition_id_);
        bool should_finish = true;
        for (auto &ready : dtxn->par_output_received_) {
          if (ready.second == false) {
            Log_info("Should not finish, output for par %u not received", ready.first);
            should_finish = false;
          }
        }
        if (should_finish) {
          Log_info("txn id = %lu Should finish", txn_id);
          dtxn->slog_phase_ = TxSlog::SLOG_DIST_OUTPUT_READY;
          dtxn->par_output_received_[partition_id_] = true;
          dtxn->CheckSendOutputToClient();
        }
      }
    }
  }
}
void SchedulerSlog::InsertBatch(TxSlog *dtxn) {
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  verify(dtxn != nullptr);

  auto now = std::chrono::system_clock::now();
  uint64_t now_ts = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

  Log_info("Inserting dtxn %lu", dtxn->id());
  output_batches_.emplace_back(std::pair<txnid_t, TxnOutput>(dtxn->id(), *(dtxn->dist_output_)));
  if (now_ts - batch_start_time_ > batch_interval_){
    commo()->SendBatchOutput(output_batches_, this->partition_id_);
    output_batches_.clear();

    auto now = std::chrono::system_clock::now();
    uint64_t now_ts = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    batch_start_time_ = now_ts;
  }
  else{
    Log_info("batch time = %d ms", (now_ts-batch_start_time_) / 1000);
  }
}
void SchedulerSlog::CheckBatchLoop() {
  while(true){
    std::unique_lock<std::recursive_mutex> lk(mtx_);
    if (commo_ != nullptr){
      auto now = std::chrono::system_clock::now();
      uint64_t now_ts = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
      if (!output_batches_.empty() && now_ts - batch_start_time_ > batch_interval_){
        commo()->SendBatchOutput(output_batches_, this->partition_id_);
        output_batches_.clear();

        auto now = std::chrono::system_clock::now();
        uint64_t now_ts = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
        batch_start_time_ = now_ts;
      }
    }
    lk.unlock();
    std::this_thread::sleep_for(std::chrono::microseconds(batch_interval_));
  }
}

