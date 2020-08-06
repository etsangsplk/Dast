//
// Created by micha on 2020/3/23.
//

#pragma once
#include "brq/commo.h"

namespace rococo {

class ChronosCommo : public BrqCommo {
 public:
  using BrqCommo::BrqCommo;

  void SendHandoutRo(SimpleCommand& cmd,
                     const function<void(int res,
                                         SimpleCommand& cmd,
                                         map<int, mdb::version_t>& vers)>&)
  override;


  //xs's code
  void SubmitLocalReq(vector<SimpleCommand>& cmd,
                    const ChronosSubmitReq& req,
                    const function<void(TxnOutput& output,
                                        ChronosSubmitRes &chr_res)>&)  ;

  void SubmitDistributedReq(map<parid_t, vector<SimpleCommand>>& cmd,
                      parid_t home_partition,
                      const ChronosSubmitReq& req,
                      const function<void(TxnOutput& output,
                                          ChronosSubmitRes &chr_res)>&)  ;

//  void BroadcastLocalSync(const ChronosLocalSyncReq& req,
//                          const function<void(ChronosLocalSyncRes& res)>& callback);

  void SendLocalSync(siteid_t target_site,
                          const ChronosLocalSyncReq& req,
                          const function<void(ChronosLocalSyncRes& res)>& callback);

  void SendStoreLocal(siteid_t target_site,
                    const vector<SimpleCommand>& cmd,
                    const ChronosStoreLocalReq& req,
                    const function<void(ChronosStoreLocalRes &chr_res)>&)  ;

  void SendProposeRemote(const vector<SimpleCommand>& cmd,
                        const ChronosProposeRemoteReq &req,
                        const function<void(uint16_t target_site, ChronosProposeRemoteRes &chr_res)>&) ;


  void SendProposeLocal(siteid_t target_site,
                         const vector<SimpleCommand>& cmd,
                         const ChronosProposeLocalReq &req,
                         const function<void(ChronosProposeLocalRes &chr_res)>&) ;

  void SendStoreRemote(siteid_t target_site,
                         const vector<SimpleCommand>& cmd,
                         const ChronosStoreRemoteReq &req,
                         const function<void(ChronosStoreRemoteRes &chr_res)>&) ;

  void SendRemotePrepared(siteid_t target_site,
                       const ChronosRemotePreparedReq &req);

  void SendOutput(parid_t target_partition,
                  const ChronosSendOutputReq &req);


  void SendDistExe(parid_t par_id, const ChronosDistExeReq &chr_req,
                     const function<void(TxnOutput &output, ChronosDistExeRes &chr_res)>&);

};

} // namespace


