//
// Created by micha on 2020/3/23.
//
#pragma once

#include "deptran/rcc/dtxn.h"
#include "../command.h"
#include "memdb/row_mv.h"
#include "deptran/chronos/scheduler.h"

#define LAT_BREAKDOWN

namespace rococo {


enum DistPartStatus{
  P_NOT_RECEIVED = 0,
  P_REMOTE_SENT = 1,
  P_REMOTE_PREPARED = 2,
  P_LOCAL_NOT_SENT = 3,
  P_LOCAL_SENT = 4,
  P_LOCAL_PREPARED = 5,
  P_OUTPUT_RECEIVED = 6,
  P_ABORTED = -1,
};
//
//enum DistTxnPhase
//    {T_REMOTE_SENT= 0, T_REMOTE_PREPARED = 1, T_LOCAL_PREPARED = 2, T_NOT_DISTRIBUTED = -1};

enum TxnPhase {
  PHASE_EMPTY = 0,
  DIST_REMOTE_SENT = 1,
  DIST_REMOTE_PREPARED = 2,
  LOCAL_NOT_STORED = 3,
  LOCAL_CAN_EXE = 4,
  DIST_CAN_EXE = 5,
  OUTPUT_SENT = 6,
};


const int PHASE_CAN_EXE = 4;  // >= PHASE_CAN_EXE can be executed

class TxChronos : public RccDTxn {
 public:
  using RccDTxn::RccDTxn;


  virtual mdb::Row *CreateRow(
      const mdb::Schema *schema,
      const std::vector<mdb::Value> &values) override {
    Log_info("[[%s]] called", __PRETTY_FUNCTION__);
    return ChronosRow::create(schema, values);
  }

  ~TxChronos(){
    if (own_dist_output_){
      delete dist_output_;
    }

  }

  void DispatchExecute(const SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output) override;

  void PreAcceptExecute(const SimpleCommand &cmd,
                       int *res,
                       map<int32_t, Value> *output);

  void CommitExecute() override;

  bool ReadColumn(mdb::Row *row,
                  mdb::column_id_t col_id,
                  Value *value,
                  int hint_flag = TXN_INSTANT) override;

  bool WriteColumn(Row *row,
                   column_id_t col_id,
                   const Value &value,
                   int hint_flag = TXN_INSTANT) override;



  chr_ts_t ts_;

  //return whether the txn has finished
  std::function<bool()> execute_callback_ = [this](){
    Log_fatal("execution call back not assigned, id = %lu", this->tid_);
    verify(0);
    return false;
  };

  int n_local_store_acks = 0;
//  bool local_stored_ = false;


  std::set<ChronosRow *> locked_rows_ = {};


  int64_t received_prepared_ts_left_ = 0;
  int64_t received_prepared_ts_right_ = 0;

  map<ChronosRow*, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> prepared_read_ranges_ = {};
  map<ChronosRow*, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> prepared_write_ranges_ = {};



  int64_t received_dispatch_ts_left_ = 0;
  int64_t received_dispatch_ts_right_ = 0;
  map<ChronosRow*, map<column_id_t, pair<mdb::version_t, mdb::version_t>>> dispatch_ranges_ = {};

  int64_t local_prepared_ts_left_;
  int64_t local_prepared_ts_right_;

  int64_t commit_ts_;

  cmdtype_t root_type = 0;


  std::map<parid_t, DistPartStatus> dist_partition_status_;
  std::map<parid_t, std::set<siteid_t>> dist_partition_acks_;
  std::map<parid_t, chr_ts_t> anticipated_ts_;

  std::function<void()> remote_prepared_callback_ = [this](){
    Log_fatal("remote prepared call back not assigned, id = %lu", this->tid_);
    verify(0);
  };

  std::function<void()> send_output_to_handler_ = [this](){
    Log_info("I am not regional leader, no need to send output", this->tid_);
  };

  std::function<void()> send_output_client = [this](){
    Log_fatal("send to client callback not assigned, id = %lu", this->tid_);
    verify(0);
  };

  std::map<parid_t, std::map<varid_t, Value>> RecursiveExecuteReadyPieces(parid_t my_par_id);
  void CheckSendOutputToClient();
  std::set<parid_t> GetOutputsTargets(std::map<int32_t, Value> *output);
  bool MergeCheckReadyPieces(const std::map<int32_t, Value>& output);
  bool MergeCheckReadyPieces(const TxnOutput& output);

  std::vector<SimpleCommand> pieces_;
  int n_local_pieces_;
  int n_executed_pieces = 0;

  TxnPhase chr_phase_ = PHASE_EMPTY;
  int n_touched_partitions = -1;
  siteid_t handler_site_ = -1;
  siteid_t region_leader_id_ = -1;

  TxnOutput* dist_output_;
  bool own_dist_output_ = false;  //xsTodo: use smart pointer

  bool exe_tried_ = false;

#ifdef LAT_BREAKDOWN
  int64_t handler_submit_ts_;
  int64_t handler_remote_prepared_ts;
  int64_t handler_local_prepared_ts;
  int64_t handler_local_start_exe_ts;
  int64_t handler_local_exe_finish_ts;
  int64_t handler_output_ready_ts;
#endif //LAT_BREAKDOWN

};

} // namespace janus



