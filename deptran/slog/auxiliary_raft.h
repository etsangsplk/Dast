//
// Created by tyycxs on 2020/5/30.
//

#ifndef ROCOCO_DEPTRAN_AUXILIARY_RAFT_H
#define ROCOCO_DEPTRAN_AUXILIARY_RAFT_H

#include "deptran/rcc_rpc.h"
#include "deptran/slog/raft_commo.h"

class SimpleCommand;

namespace rococo{

class AuxiliaryRaftImpl : public SlogRaftService {
  void SlogRaftSubmit(const std::map<uint32_t, std::vector<SimpleCommand>> &cmds_by_par,
                      const siteid_t &handler_site,
                      int32_t *res,
                      rrr::DeferredReply *defer) override;

  void RaftAppendAck(const std::map<uint32_t, std::vector<SimpleCommand>> &cmds_by_par,
      int32_t res, uint64_t counter, siteid_t handler_site);

  void RaftAppendEntries(const std::map<uint32_t, std::vector<SimpleCommand>> &cmds_by_par,
                      rrr::i32 *res,
                      rrr::DeferredReply *defer) override;

  void SendToAllPars (const std::map<uint32_t, std::vector<SimpleCommand>> &cmds_by_par,
                      const std::vector<parid_t> & touched_pars,
                      siteid_t handler_site,
                      uint64_t index);

public:
  AuxiliaryRaftImpl(std::string proc_name): my_proc_name_ (proc_name){
    auto global_cfg = Config::GetConfig();
    n_pars_ = global_cfg->replica_groups_.size();
    Log_info("Creating AuxiliaryRaft, n_par = %d", n_pars_);
  };

  void SetupCommo(){
    raft_commo_ = new RaftCommo;
  }
  std::string my_proc_name_;

  std::recursive_mutex mu_;

  RaftCommo* raft_commo_;

  uint64_t counter_ = 0;

  std::set<uint64_t> committed_indices_ = {};

  //Output not going through the raft
//  std::map<uint64_t, TxnOutput *> output_by_index_;
//  std::map<uint64_t, int> n_output_by_index_;

  int n_pars_;

};

} //namespace rococo;

#endif //ROCOCO_DEPTRAN_AUXILIARY_RAFT_H
