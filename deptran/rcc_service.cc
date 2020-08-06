#include <deptran/ocean_vista/ov-scheduler.h>
#include "__dep__.h"
#include "config.h"
#include "scheduler.h"
#include "command.h"
#include "txn_chopper.h"
#include "command_marshaler.h"
#include "rcc/dep_graph.h"
#include "rcc_service.h"
#include "classic/sched.h"
#include "tapir/sched.h"
#include "rcc/sched.h"
#include "brq/sched.h"
#include "benchmark_control_rpc.h"
#include "chronos/scheduler.h"
#include "ocean_vista/ov-scheduler.h"
#include "deptran/slog/scheduler.h"

namespace rococo {

ClassicServiceImpl::ClassicServiceImpl(Scheduler *sched,
                                       rrr::PollMgr* poll_mgr,
                                       ServerControlServiceImpl *scsi)
    : scsi_(scsi), dtxn_sched_(sched) {

#ifdef PIECE_COUNT
  piece_count_timer_.start();
  piece_count_prepare_fail_ = 0;
  piece_count_prepare_success_ = 0;
#endif

  if (Config::GetConfig()->do_logging()) {
    auto path = Config::GetConfig()->log_path();
    recorder_ = new Recorder(path);
    poll_mgr->add(recorder_);
  }

  this->RegisterStats();
}

void ClassicServiceImpl::Dispatch(const vector<SimpleCommand>& cmd,
                                  rrr::i32 *res,
                                  TxnOutput* output,
                                  rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);

#ifdef PIECE_COUNT
  piece_count_key_t piece_count_key =
      (piece_count_key_t){header.t_type, header.p_type};
  std::map<piece_count_key_t, uint64_t>::iterator pc_it =
      piece_count_.find(piece_count_key);

  if (pc_it == piece_count_.end())
      piece_count_[piece_count_key] = 1;
  else
      piece_count_[piece_count_key]++;
  piece_count_tid_.insert(header.tid);
#endif

//  output->resize(output_size);
  // find stored procedure, and run it
  *res = SUCCESS;
  verify(cmd.size() > 0);
  dtxn_sched()->OnDispatch(cmd,
                           res,
                           output,
                           [defer] () {defer->reply();});
}

void ClassicServiceImpl::Prepare(const rrr::i64 &tid,
                                 const std::vector<i32> &sids,
                                 rrr::i32 *res,
                                 rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);
  auto sched = (ClassicSched*)dtxn_sched_;
  sched->OnPrepare(tid, sids, res, [defer] () {defer->reply();});
//  auto *dtxn = (TPLDTxn *) dtxn_sched_->get(tid);
//  dtxn->prepare_launch(sids, res, defer);
// TODO move the stat to somewhere else.
#ifdef PIECE_COUNT
  std::map<piece_count_key_t, uint64_t>::iterator pc_it;
  if (*res != SUCCESS)
      piece_count_prepare_fail_++;
  else
      piece_count_prepare_success_++;
  if (piece_count_timer_.elapsed() >= 5.0) {
      Log::info("PIECE_COUNT: txn served: %u", piece_count_tid_.size());
      Log::info("PIECE_COUNT: prepare success: %llu, failed: %llu",
        piece_count_prepare_success_, piece_count_prepare_fail_);
      for (pc_it = piece_count_.begin(); pc_it != piece_count_.end(); pc_it++)
          Log::info("PIECE_COUNT: t_type: %d, p_type: %d, started: %llu",
            pc_it->first.t_type, pc_it->first.p_type, pc_it->second);
      piece_count_timer_.start();
  }
#endif
}

void ClassicServiceImpl::Commit(const rrr::i64 &tid,
                                rrr::i32 *res,
                                rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(mtx_);
  auto sched = (ClassicSched*) dtxn_sched_;
  sched->OnCommit(tid, SUCCESS, res, [defer] () {defer->reply();});
}

void ClassicServiceImpl::Abort(const rrr::i64 &tid,
                               rrr::i32 *res,
                               rrr::DeferredReply *defer) {
  Log::debug("get abort_txn: tid: %ld", tid);
  auto sched = (ClassicSched*) dtxn_sched_;
  sched->OnCommit(tid, REJECT, res, [defer] () {defer->reply();});
}


void ClassicServiceImpl::rpc_null(rrr::DeferredReply *defer) {
  defer->reply();
}



void ClassicServiceImpl::UpgradeEpoch(const uint32_t& curr_epoch,
                                  int32_t *res,
                                  DeferredReply* defer) {
  *res = dtxn_sched()->OnUpgradeEpoch(curr_epoch);
  defer->reply();
}

void ClassicServiceImpl::TruncateEpoch(const uint32_t& old_epoch,
                                   DeferredReply* defer) {
  std::function<void()> func = [&] () {
    dtxn_sched()->OnTruncateEpoch(old_epoch);
    defer->reply();
  };
  Coroutine::Create(func);
}

void ClassicServiceImpl::TapirAccept(const cmdid_t& cmd_id,
                              const ballot_t& ballot,
                              const int32_t& decision,
                              rrr::DeferredReply* defer) {
  verify(0);
}

void ClassicServiceImpl::TapirFastAccept(const cmdid_t& cmd_id,
                                  const vector<SimpleCommand>& txn_cmds,
                                  rrr::i32* res,
                                  rrr::DeferredReply* defer) {
  TapirSched* sched = (TapirSched*)dtxn_sched_;
  sched->OnFastAccept(cmd_id, txn_cmds, res,
                       [defer] () {defer->reply();});
}

void ClassicServiceImpl::TapirDecide(const cmdid_t& cmd_id,
                                     const rrr::i32& decision,
                                     rrr::DeferredReply* defer) {
  TapirSched* sched = (TapirSched*)dtxn_sched_;
  sched->OnDecide(cmd_id, decision, [defer] () {defer->reply();});
}

void ClassicServiceImpl::RccDispatch(const vector<SimpleCommand>& cmd,
                                    int32_t* res,
                                    TxnOutput* output,
                                    RccGraph* graph,
                                    DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(this->mtx_);
  RccSched* sched = (RccSched*) dtxn_sched_;
  sched->OnDispatch(cmd,
                    res,
                    output,
                    graph,
                    [defer]() { defer->reply(); });
}

void ClassicServiceImpl::RccFinish(const cmdid_t& cmd_id,
                                   const RccGraph& graph,
                                   TxnOutput* output,
                                   DeferredReply* defer) {
  verify(graph.size() > 0);
  std::lock_guard <std::mutex> guard(mtx_);
  RccSched* sched = (RccSched*) dtxn_sched_;
  sched->OnCommit(cmd_id,
                  graph,
                  output,
                  [defer]() { defer->reply(); });

  stat_sz_gra_commit_.sample(graph.size());
}

void ClassicServiceImpl::RccInquire(const epoch_t& epoch,
                                    const txnid_t& tid,
                                    RccGraph *graph,
                                    rrr::DeferredReply *defer) {
  verify(IS_MODE_RCC || IS_MODE_RO6);
  std::lock_guard <std::mutex> guard(mtx_);
  RccSched* sched = (RccSched*) dtxn_sched_;
  sched->OnInquire(epoch, tid, graph, [defer]() { defer->reply(); });
}

void ClassicServiceImpl::RccDispatchRo(const SimpleCommand &cmd,
                                       map <int32_t, Value> *output,
                                       rrr::DeferredReply *defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  verify(0);
  RccDTxn *dtxn = (RccDTxn *) dtxn_sched_->GetOrCreateDTxn(cmd.root_id_, true);
  dtxn->start_ro(cmd, *output, defer);
}

void ClassicServiceImpl::BrqDispatch(const vector<SimpleCommand>& cmd,
                                     int32_t* res,
                                     TxnOutput* output,
                                     Marshallable* res_graph,
                                     DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(this->mtx_);

  RccGraph* tmp_graph = new RccGraph();
  BrqSched* sched = (BrqSched*) dtxn_sched_;
  sched->OnDispatch(cmd,
                    res,
                    output,
                    tmp_graph,
                    [defer, tmp_graph, res_graph] () {
                      if (tmp_graph->size() <= 1) {
                        res_graph->rtti_ = Marshallable::EMPTY_GRAPH;
                        res_graph->ptr().reset(new EmptyGraph);
                        delete tmp_graph;
                      } else {
                        res_graph->rtti_ = Marshallable::RCC_GRAPH;
                        res_graph->ptr().reset(tmp_graph);
                      }
                      defer->reply();
                    });
}

void ClassicServiceImpl::BrqCommit(const cmdid_t& cmd_id,
                                   const Marshallable& graph,
                                   int32_t *res,
                                   TxnOutput* output,
                                   DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  BrqSched* sched = (BrqSched*) dtxn_sched_;
  sched->OnCommit(cmd_id,
                  dynamic_cast<RccGraph*>(graph.ptr().get()),
                  res,
                  output,
                  [defer]() { defer->reply(); });
}

void ClassicServiceImpl::BrqCommitWoGraph(const cmdid_t& cmd_id,
                                       int32_t *res,
                                       TxnOutput* output,
                                       DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  BrqSched* sched = (BrqSched*) dtxn_sched_;
  sched->OnCommit(cmd_id,
                         nullptr,
                         res,
                         output,
                         [defer]() { defer->reply(); });
}

void ClassicServiceImpl::BrqInquire(const epoch_t& epoch,
                                    const cmdid_t &tid,
                                    Marshallable *graph,
                                    rrr::DeferredReply *defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  graph->rtti_ = Marshallable::RCC_GRAPH;
  graph->ptr().reset(new RccGraph());
  BrqSched* sched = (BrqSched*) dtxn_sched_;
  sched->OnInquire(epoch,
                   tid,
                   dynamic_cast<RccGraph*>(graph->ptr().get()),
                   [defer]() { defer->reply(); });
}

void ClassicServiceImpl::BrqPreAccept(const cmdid_t &txnid,
                                      const vector<SimpleCommand>& cmds,
                                      const Marshallable& graph,
                                      int32_t* res,
                                      Marshallable* res_graph,
                                      DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  verify(dynamic_cast<RccGraph*>(graph.ptr().get()));
  verify(graph.rtti_ == Marshallable::RCC_GRAPH);
  res_graph->rtti_ = Marshallable::RCC_GRAPH;
  res_graph->ptr().reset(new RccGraph());
  BrqSched* sched = (BrqSched*) dtxn_sched_;
  sched->OnPreAccept(txnid,
                     cmds,
                     dynamic_cast<RccGraph&>(*graph.ptr().get()),
                     res,
                     dynamic_cast<RccGraph*>(res_graph->ptr().get()),
                     [defer] () {defer->reply();});
}

void ClassicServiceImpl::BrqPreAcceptWoGraph(const cmdid_t& txnid,
                                             const vector<SimpleCommand>& cmds,
                                             int32_t* res,
                                             Marshallable* res_graph,
                                             DeferredReply* defer) {
  std::lock_guard <std::mutex> guard(mtx_);
  res_graph->rtti_ = Marshallable::RCC_GRAPH;
  res_graph->ptr().reset(new RccGraph());
  BrqSched* sched = (BrqSched*) dtxn_sched_;
  sched->OnPreAcceptWoGraph(txnid,
                            cmds,
                            res,
                            dynamic_cast<RccGraph*>(res_graph->ptr().get()),
                            [defer] () {defer->reply();});
}


void ClassicServiceImpl::BrqAccept(const cmdid_t &txnid,
                                   const ballot_t& ballot,
                                   const Marshallable& graph,
                                   int32_t* res,
                                   DeferredReply* defer) {
  verify(dynamic_cast<RccGraph*>(graph.ptr().get()));
  verify(graph.rtti_ == Marshallable::RCC_GRAPH);
  BrqSched* sched = (BrqSched*) dtxn_sched_;
  sched->OnAccept(txnid,
                  ballot,
                  dynamic_cast<RccGraph&>(*graph.ptr().get()),
                  res,
                  [defer] () {defer->reply();});
}


void ClassicServiceImpl::RegisterStats() {
  if (scsi_) {
    scsi_->set_recorder(recorder_);
    scsi_->set_recorder(recorder_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_SZ_SCC,
                    &stat_sz_scc_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_START,
                    &stat_sz_gra_start_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_COMMIT,
                    &stat_sz_gra_commit_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_SZ_GRAPH_ASK,
                    &stat_sz_gra_ask_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_N_ASK,
                    &stat_n_ask_);
    scsi_->set_stat(ServerControlServiceImpl::STAT_RO6_SZ_VECTOR,
                    &stat_ro6_sz_vector_);
  }
}


void ClassicServiceImpl::ChronosSubmitLocal(const vector<SimpleCommand> &cmd,
                                         const ChronosSubmitReq &req,
                                         ChronosSubmitRes *chr_res,
                                         TxnOutput *p_output,
                                         DeferredReply *defer) {

//  Log_info("%s called", __PRETTY_FUNCTION__);

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnSubmitLocal(cmd, req, chr_res, p_output, [defer]() { defer->reply(); });


//  Log_info("%s returned", __PRETTY_FUNCTION__);

}


void ClassicServiceImpl::ChronosSubmitDistributed(const std::map<parid_t, std::vector<SimpleCommand>> &cmds_by_par,
                              const ChronosSubmitReq &req,
                              ChronosSubmitRes *chr_res,
                              TxnOutput *output,
                              rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnSubmitDistributed(cmds_by_par, req, chr_res, output, [defer]() { defer->reply(); });
}

void ClassicServiceImpl::ChronosSendOutput(const ChronosSendOutputReq &chr_req, ChronosSendOutputRes *chr_res, rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnSendOutput(chr_req, chr_res, [defer](){defer->reply();});

}


void ClassicServiceImpl::ChronosStoreLocal(const vector<SimpleCommand> &cmd,
                                         const ChronosStoreLocalReq &req,
                                         ChronosStoreLocalRes *chr_res,
                                         DeferredReply *p_defer) {


  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnStoreLocal(cmd, req,  chr_res, [p_defer](){p_defer->reply();});

}

void ClassicServiceImpl::ChronosLocalSync(const ChronosLocalSyncReq &req, ChronosLocalSyncRes *res, rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnSync(req, res, [defer](){defer->reply();});
}

void ClassicServiceImpl:: ChronosStoreRemote(const std::vector<SimpleCommand> &cmd,
                           const ChronosStoreRemoteReq &chr_req,
                           ChronosStoreRemoteRes *chr_res,
                           rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnStoreRemote(cmd, chr_req, chr_res, [defer](){defer->reply();});
}

void ClassicServiceImpl::ChronosRemotePrepared(
                        const ChronosRemotePreparedReq &req,
                        ChronosRemotePreparedRes *chr_res,
                        rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnRemotePrepared(req, chr_res, [defer](){defer->reply();});

}

void ClassicServiceImpl::ChronosProposeRemote(const std::vector<SimpleCommand> &cmd,
                          const ChronosProposeRemoteReq &req,
                          ChronosProposeRemoteRes *chr_res,
                          rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnProposeRemote(cmd, req, chr_res , [defer](){defer->reply();});
}

void ClassicServiceImpl::ChronosDistExe(const ChronosDistExeReq &chr_req,
                                          ChronosDistExeRes *chr_res,
                                          TxnOutput *output,
                                          rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnDistExe(chr_req, chr_res, output, [defer](){defer->reply();});

}


void ClassicServiceImpl::ChronosProposeLocal(const std::vector<SimpleCommand> &cmd,
                         const ChronosProposeLocalReq &req,
                         ChronosProposeLocalRes *chr_res,
                         rrr::DeferredReply *defer) {
  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerChronos *) dtxn_sched_;

  sched->OnProposeLocal(cmd, req,  chr_res , [defer](){defer->reply();});
}



void ClassicServiceImpl::OVStore(const cmdid_t &txn_id,
             const std::vector<SimpleCommand> &cmds,
             const OVStoreReq &ov_req,
             rrr::i32 *res,
             OVStoreRes *ov_res,
             rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(mtx_);
  SchedulerOV *sched = (SchedulerOV *) dtxn_sched_;
  sched->OnStore(txn_id,
                     cmds,
                     ov_req,
                     res,
                     ov_res);
  defer->reply();
}

void ClassicServiceImpl::OVCreateTs(const cmdid_t &txn_id,
                rrr::i64 *timestamp,
                rrr::i16 *site_id,
                rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(mtx_);
  SchedulerOV *sched = (SchedulerOV *) dtxn_sched_;

  sched->OnCreateTs(txn_id, timestamp, site_id);

  defer->reply();

}

void ClassicServiceImpl::OVStoredRemoveTs(const cmdid_t &txn_id,
                                          const rrr::i64 &timestamp,
                                          const rrr::i16 &site_id,
                                          int32_t *res,
                                          rrr::DeferredReply *defer){

  std::lock_guard<std::mutex> guard(mtx_);
  SchedulerOV *sched = (SchedulerOV *) dtxn_sched_;

  sched->OnStoredRemoveTs(txn_id, timestamp, site_id, res);

  defer->reply();
}
void ClassicServiceImpl::OVExecute(const uint64_t &id,
                                   const OVExecuteReq &req,
                                   int32_t *res,
                                   OVExecuteRes *ov_res,
                                   TxnOutput *output,
                                   rrr::DeferredReply *defer) {

  std::unique_lock<std::mutex> lk(mtx_);
  SchedulerOV *sched = (SchedulerOV *) dtxn_sched_;
//  lk.unlock();

  auto callback = [defer](){
   defer->reply();
  };

  sched->OnExecute(id, req, res, ov_res, output, callback);

}

void ClassicServiceImpl::OVPublish(const rrr::i64 &dc_timestamp,
                                   const rrr::i16 &dc_site_id,
                                   rrr::i64 *ret_timestamp,
                                   rrr::i16 *ret_site_id,
                                   rrr::DeferredReply *defer) {

  std::unique_lock<std::mutex> lk(mtx_);
  SchedulerOV *sched = (SchedulerOV *) dtxn_sched_;
  sched->OnPublish(dc_timestamp, dc_site_id, ret_timestamp, ret_site_id);

  defer->reply();
}

void ClassicServiceImpl::OVExchange(const std::string &source_dcname,
                                    const rrr::i64 &dvw_timestamp,
                                    const rrr::i16 &dvw_site_id,
                                    rrr::i64 *ret_timestamp,
                                    rrr::i16 *ret_site_id,
                                    rrr::DeferredReply *defer) {

  std::unique_lock<std::mutex> lk(mtx_);
  SchedulerOV *sched = (SchedulerOV *) dtxn_sched_;
  sched->OnExchange(source_dcname, dvw_timestamp, dvw_site_id, ret_timestamp, ret_site_id);

  defer->reply();
}

void ClassicServiceImpl::OVDispatch(const vector<SimpleCommand> &cmd,
                                         int32_t *p_res,
                                         TxnOutput *p_output,
                                         DeferredReply *p_defer) {

//  Log_info("%s called", __PRETTY_FUNCTION__);

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  auto *sched = (SchedulerOV *) dtxn_sched_;

  sched->OnDispatch(cmd, p_res, p_output);


  p_defer->reply();

//  Log_info("%s returned", __PRETTY_FUNCTION__);

}

void ClassicServiceImpl::SlogInsertDistributed(const std::vector<SimpleCommand> &cmd,
                           const uint64_t &index,
                           const std::vector<parid_t>& touched_pars,
                           const siteid_t &handler_site,
                           int32_t *res,
                           rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  Log_info("%s called", __FUNCTION__);
  auto *sched = (SchedulerSlog *) dtxn_sched_;
  *res = SUCCESS;

  sched->OnInsertDistributed(cmd, index, touched_pars, handler_site);
  defer->reply();
}
void ClassicServiceImpl::SlogSubmitLocal(const vector<SimpleCommand> &cmd,
                                         TxnOutput *output,
                                         rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  Log_info("%s called", __FUNCTION__);
  auto *sched = (SchedulerSlog *) dtxn_sched_;

  sched->OnSubmitLocal(cmd, output, [defer]{defer->reply();});
}
void ClassicServiceImpl::SlogReplicateLogLocal(const vector<SimpleCommand> &cmd,
                                               const uint64_t &index,
                                               const uint64_t &commit_index,
                                               rrr::i32 *res,
                                               rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  Log_info("%s called", __FUNCTION__);
  auto *sched = (SchedulerSlog *) dtxn_sched_;
  *res = SUCCESS;
  sched->OnReplicateLocal(cmd, index, commit_index, [defer]{defer->reply();});
}
void ClassicServiceImpl::SlogSubmitDistributed(const map<uint32_t, std::vector<SimpleCommand>> &cmds_by_par,
                                               TxnOutput *output,
                                               rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  Log_info("%s called", __FUNCTION__);
  auto *sched = (SchedulerSlog *) dtxn_sched_;
  sched->OnSubmitDistributed(cmds_by_par, output, [defer]{defer->reply();});
}

void ClassicServiceImpl::SlogSendBatchRemote(const vector<pair<uint64_t, TxnOutput>> &batch,
                                             const parid_t &my_par,
                                             rrr::i32 *res,
                                             rrr::DeferredReply *defer) {

  std::lock_guard<std::mutex> guard(this->mtx_); // TODO remove the lock.
  Log_info("%s called", __FUNCTION__);
  auto *sched = (SchedulerSlog *) dtxn_sched_;
  *res = SUCCESS;
  sched->OnSendBatchRemote(my_par, batch);
  defer->reply();
}
} // namespace rcc
