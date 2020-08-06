#include "piece.h"
#include <limits>

namespace rococo {

using rococo::RccDTxn;

char TPCC_TB_WAREHOUSE[] =    "warehouse";
char TPCC_TB_DISTRICT[] =     "district";
char TPCC_TB_CUSTOMER[] =     "customer";
char TPCC_TB_HISTORY[] =      "history";
char TPCC_TB_ORDER[] =        "order";
char TPCC_TB_NEW_ORDER[] =    "new_order";
char TPCC_TB_ITEM[] =         "item";
char TPCC_TB_STOCK[] =        "stock";
char TPCC_TB_ORDER_LINE[] =   "order_line";
char TPCC_TB_ORDER_C_ID_SECONDARY[] = "order_secondary";
char TPCC_TB_REGION[] = "region";
char TPCC_TB_NATION[] = "nation";
char TPCC_TB_SUPPLIER[] = "supplier";

void TpccPiece::reg_all() {
    RegNewOrder();
    RegPayment();
    RegOrderStatus();
    RegDelivery();
    RegStockLevel();
    RegQuery2();
}

TpccPiece::TpccPiece() {}

TpccPiece::~TpccPiece() {}

}
