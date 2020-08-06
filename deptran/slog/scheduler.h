//
// Created by micha on 2020/3/23.
//


#pragma once
#include "deptran/brq/sched.h"
#include "deptran/rcc_rpc.h"
#include "deptran/slog/tx.h"
namespace rococo {

class TxSlog;
class SlogCommo;

class SchedulerSlog : public Scheduler {
 public:


  SchedulerSlog(Frame* frame);


  int OnSubmitLocal(const vector<SimpleCommand> &cmd,
                    TxnOutput* output,
                    const function<void()> &callback);


  int OnInsertDistributed(const vector<SimpleCommand> &cmds,
                          uint64_t index,
                          const std::vector<parid_t>& touched_pars,
                          siteid_t handler_site);

  void OnSubmitDistributed(const map<parid_t, vector<SimpleCommand>> &cmds_by_par,
                          TxnOutput* output,
                          const function<void()> &reply_callback);


  void ReplicateLogLocalAck(uint64_t index);



  void OnReplicateLocal(const vector<SimpleCommand> &cmd,
               uint64_t index,
               uint64_t commit_index,
               const function<void()> &callback);


  void OnSendOutput(const ChronosSendOutputReq &chr_req,
                    ChronosSendOutputRes *chr_res,
                    const function<void()>& callback);

  void OnSendBatchRemote(parid_t src_par,
                         const vector<pair<uint64_t, TxnOutput>>& batch);

  void InsertBatch(TxSlog *dtxn);

  void CheckBatchLoop();


  std::thread check_batch_loop_thread;

  std::vector<TxSlog*> txn_log_ = {};

  std::vector<std::unique_ptr<vector<SimpleCommand>>> global_log_ = {};
  uint64_t global_log_next_index_ = 0;

  SlogCommo* commo();


  std::vector<std::pair<txnid_t, TxnOutput>> output_batches_ = {};
  int batch_size_ = 1;
  int batch_interval_ = 10 * 1000;
  uint64_t batch_start_time_ = 0;

  map<txnid_t, uint64_t> id_to_index_map_ = {};


//  uint64_t received_global_index_ = -1;
  int n_replicas_;
  uint64_t local_log_commited_index_ = -1;
  uint64_t backup_executed_index_ = -1;

  std::set<siteid_t> follower_sites_;

};
} // namespace janus
