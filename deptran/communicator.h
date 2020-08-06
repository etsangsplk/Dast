#pragma once

#include <chrono>
#include <atomic>
#include "__dep__.h"
#include "constants.h"
#include "msg.h"
#include "config.h"
#include "command_marshaler.h"
#include "deptran/rcc/dep_graph.h"
#include "rcc_rpc.h"

namespace rococo {

class Coordinator;
class ClassicProxy;
class ClientControlProxy;

typedef std::pair<siteid_t, ClassicProxy*> SiteProxyPair;
typedef std::pair<siteid_t, ClientControlProxy*> ClientSiteProxyPair;

class Communicator {
 public:
  const int CONNECT_TIMEOUT_MS = 120*1000;
  const int CONNECT_SLEEP_MS = 1000;
  rrr::PollMgr *rpc_poll_ = nullptr;
  locid_t loc_id_ = -1;
  std::string dcname_;
  Config::SiteInfo* site_info_ = nullptr;

  map<siteid_t, rrr::Client *> rpc_clients_ = {};
  map<siteid_t, ClassicProxy *> rpc_proxies_ = {};
  map<parid_t, vector<SiteProxyPair>> rpc_par_proxies_ = {};
  map<std::string, vector<SiteProxyPair>> rpc_dc_proxies_ = {};
  map<parid_t, SiteProxyPair> leader_cache_ = {};
  vector<ClientSiteProxyPair> client_leaders_;
  std::atomic_bool client_leaders_connected_;
  std::vector<std::thread> threads;

//  rrr::Client *raft_client_ = nullptr;
  rrr::PollMgr *raft_rpc_poll_ = nullptr;
//  SlogRaftProxy *raft_proxy_ = nullptr;

  std::vector<rrr::Client*> raft_clients_;
  std::vector<SlogRaftProxy*> raft_proxies_;

  Communicator(PollMgr* poll_mgr = nullptr);
  virtual ~Communicator();

  SiteProxyPair RandomProxyForPartition(parid_t partition_id) const;
  SiteProxyPair LeaderProxyForPartition(parid_t) const;
  SiteProxyPair NearestProxyForPartition(parid_t) const;
  SiteProxyPair NearestRandomProxy();
  SiteProxyPair NearestProxyForAnyPartition(const std::vector<parid_t>& par_ids) const;
  std::vector<SiteProxyPair> ProxiesInPartition(parid_t par_id);


  int last_edge_index = 0;
  SiteProxyPair EdgeServerForPartition(parid_t);

  std::pair<int, ClassicProxy*> ConnectToSite(rococo::Config::SiteInfo &site, std::chrono::milliseconds timeout_ms);
  ClientSiteProxyPair ConnectToClientSite(Config::SiteInfo &site, std::chrono::milliseconds timeout);
  void ConnectClientLeaders();
  void WaitConnectClientLeaders();
  void ConnectToRaft();
};

} // namespace rococo
