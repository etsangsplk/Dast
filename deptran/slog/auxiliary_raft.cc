//
// Created by tyycxs on 2020/5/31.
//
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
#include "auxiliary_raft.h"

namespace rococo {

void AuxiliaryRaftImpl::SlogRaftSubmit(const std::map<uint32_t, std::vector<SimpleCommand>> &cmds_by_par,
                                       const siteid_t & handler_site,
                                       int32_t *res,
                                       rrr::DeferredReply *defer) {
  std::lock_guard<std::recursive_mutex> guard(mu_);
  Log_info("%s called", __FUNCTION__);

  auto index = counter_++;


  std::function<void(Future *)> cb =
      [=](Future *fu) {
        int ack_res;
        fu->get_reply() >> ack_res;
        RaftAppendAck(cmds_by_par, ack_res, index, handler_site);
      };
  rrr::FutureAttr fuattr;
  fuattr.callback = cb;

  for (int i = 1; i<raft_commo_->raft_proxies_.size(); i++){
    Log_info("raft leader sending to proxy %d", i);
    Future::safe_release(raft_commo_->raft_proxies_[i]->async_RaftAppendEntries(cmds_by_par, fuattr));
  }
  defer->reply();
  Log_info("%s returned", __FUNCTION__);

}

void AuxiliaryRaftImpl::RaftAppendAck(const std::map<uint32_t, std::vector<SimpleCommand>> &cmds_by_par,
  int32_t res, uint64_t index, siteid_t handler_site){

  Log_info("%s called", __FUNCTION__ );


  std::lock_guard<std::recursive_mutex> guard(mu_);
  if (committed_indices_.count(index) == 0){
    //only one ack is enough
    committed_indices_.insert(index);
    Log_info("index %lu committed, going to send to all par leaders", index);
    std::vector<parid_t> touched_pars;
    for (auto& pair: cmds_by_par){
      touched_pars.push_back(pair.first);
    }
    SendToAllPars(cmds_by_par, touched_pars, handler_site, index);
  }
  Log_info("%s returned", __FUNCTION__ );
}

//void AuxiliaryRaftImpl::SendToAllLeaders (){
//
//}


void AuxiliaryRaftImpl::SendToAllPars (const std::map<uint32_t, std::vector<SimpleCommand>> &cmds_by_par,
                      const std::vector<parid_t> & touched_pars,
                      siteid_t handler_site,
                      uint64_t index){

  std::lock_guard<std::recursive_mutex> guard(mu_);
  for (auto &pair: raft_commo_->rpc_par_proxies_){
    parid_t par_id = pair.first;
    if (cmds_by_par.count(par_id) != 0){
      auto proxy = pair.second.begin()->second;
      auto site_id = pair.second.begin()->first;
      Log_info("sending pieces (len = %d) to site %hu, leader of parititon %u, index = %lu",
                cmds_by_par.at(par_id).size(),
                site_id,
                par_id,
                index);
      Future::safe_release(proxy->async_SlogInsertDistributed(cmds_by_par.at(par_id), index, touched_pars, handler_site));
    }
    else{
      auto proxy = pair.second.begin()->second;
      auto site_id = pair.second.begin()->first;
      Log_info("sending skip message to site %hu, leader of parititon %u, index = %lu",
               site_id,
               par_id,
               index);
      auto skip = vector<SimpleCommand>{};
      Future::safe_release(proxy->async_SlogInsertDistributed(skip, index, touched_pars, handler_site));
    }
  }
};

void AuxiliaryRaftImpl::RaftAppendEntries(const std::map<uint32_t, std::vector<SimpleCommand>> &cmds_by_par,
                       rrr::i32 *res,
                       rrr::DeferredReply *defer) {
  std::lock_guard<std::recursive_mutex> guard(mu_);
  txnid_t txn_id = cmds_by_par.begin()->second[0].root_id_;
  Log_info("%s called for id %lu", __FUNCTION__, txn_id);
  *res = SUCCESS;
  defer->reply();
}
}