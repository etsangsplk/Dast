#pragma once

#include "../__dep__.h"
#include "../executor.h"

namespace rococo {


enum tapir_phase_t {
  tapir_dispatch,
  tapir_fast,
  tapir_decide
};

class TapirDTxn;
class TapirExecutor : public Executor {
 public:
  tapir_phase_t phase_ = tapir_dispatch;
  using Executor::Executor;
  set<VersionedRow*> locked_rows_ = {};
  static set<Row*> locked_rows_s;
  void FastAccept(const vector<SimpleCommand>& txn_cmds,
                  int32_t *res);
  void Commit();
  void Abort();

  void Execute(const vector<SimpleCommand>& cmd,
               TxnOutput* output) override ;


  TapirDTxn* dtxn();
};

} // namespace rococo