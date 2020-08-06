//
// Created by micha on 2020/3/23.
//

#include "deptran/rcc/dtxn.h"
#include "../rcc/graph_marshaler.h"
#include "commo.h"
#include "marshallable.h"
#include "txn_chopper.h"

namespace rococo {

void SlogCommo::SubmitLocalReq(vector<TxPieceData> &cmd,
                                  const function<void(TxnOutput &cmd)> &callback) {

  rrr::FutureAttr fuattr;
  auto tid = cmd[0].root_id_;
  auto par_id = cmd[0].partition_id_;
  std::function<void(Future *)> cb =
      [callback, tid, par_id](Future *fu) {
        TxnOutput output;
        fu->get_reply()  >> output;
        callback(output);
      };
  fuattr.callback = cb;
  auto proxy_info = this->rpc_par_proxies_[par_id][0];
  //xs: seems to dispatch only the nearst replica fo the shard


  auto proxy = proxy_info.second;
  //XS: proxy is the rpc client side handler.
  Log_info("dispatch local transaction %lu to partition %u, proxy (site) = %hu",
           cmd[0].root_id_,
           cmd[0].PartitionId(),
           proxy_info.first);

  Future::safe_release(proxy->async_SlogSubmitLocal(cmd, fuattr));

  Log_info("-- Submit Local returned");
}

void SlogCommo::SubmitDistributedReq(  parid_t home_par,
                                       map<parid_t, vector<SimpleCommand>> &cmds_by_par,
                                        const function<void(TxnOutput &output)> &callback) {


  //Test code


  rrr::FutureAttr fuattr;
  verify(cmds_by_par.size() > 0);
  verify(cmds_by_par.begin()->second.size() > 0);
  txnid_t txn_id = cmds_by_par.begin()->second.begin()->root_id_;
  std::function<void(Future *)> cb =
      [callback](Future *fu) {
        TxnOutput output;
        fu->get_reply()  >> output;
        callback(output);
      };
  fuattr.callback = cb;
  //XS: proxy is the rpc client side handler.

  auto leader = rpc_par_proxies_[home_par][0];


  Future::safe_release(leader.second->async_SlogSubmitDistributed(cmds_by_par, fuattr));

//  Log_info("-- SubmitReq returned");
}



void SlogCommo::SendOutput(parid_t target_partition,
                const ChronosSendOutputReq &req){
  Log_info("%s called, sending to partition %u", __FUNCTION__, target_partition);

  auto proxies = rpc_par_proxies_[target_partition];
  for (auto &proxy: proxies){
    Log_info("sending output of txn %lu to site = %hu", req.txn_id, proxy.first);
    Future::safe_release(proxy.second->async_ChronosSendOutput(req));
  }

}



void SlogCommo::SendHandoutRo(SimpleCommand &cmd,
                                 const function<void(int res,
                                                     SimpleCommand &cmd,
                                                     map<int,
                                                         mdb::version_t> &vers)> &) {
  verify(0);
}
void SlogCommo::SendReplicateLocal(uint16_t target_site,
                                   const vector<SimpleCommand> &cmd,
                                   uint64_t index,
                                   uint64_t commit_index,
                                   const function<void()> &callback) {
  Log_info("%s called, sending to partition %hu, index = %lu, commit_index = %lu", __FUNCTION__, target_site, index, commit_index);

  rrr::FutureAttr fuattr;
  std::function<void(Future *)> cb =
      [callback](Future *fu) {
        fu->get_reply();
        callback();
      };
  fuattr.callback = cb;
  //XS: proxy is the rpc client side handler.
  auto proxy = rpc_proxies_[target_site];

  Future::safe_release(proxy->async_SlogReplicateLogLocal(cmd, index, commit_index, fuattr));
}
void SlogCommo::SendToRaft(const map<uint32_t, vector<SimpleCommand>> &cmds_by_par, uint16_t handler_site) {
  verify(raft_proxies_.size() > 0);
  auto leader = raft_proxies_[0];
  Future::safe_release(leader->async_SlogRaftSubmit(cmds_by_par, handler_site));
}
void SlogCommo::SendBatchOutput(const std::vector<std::pair<uint64_t, TxnOutput>> &batch, uint32_t my_par_id) {
  Log_info("%s called", __FUNCTION__ );

  for (auto& proxy: rpc_proxies_){
    if (proxy.first != this->site_info_->id){
      Log_info("Sending batch to site %hu of %d output, first id = %lu ",
           proxy.first,
           batch.size(),
           batch[0].first);
      proxy.second->async_SlogSendBatchRemote(batch, my_par_id);
    }
  }
}

} // namespace janus
