#pragma once

#include "threadinfo.h"

using namespace NVMMgr_ns;

namespace PART_ns{
    class EpochGuard{
    public:
        EpochGuard(){
            JoinNewEpoch();
        }
        ~EpochGuard(){
            LeaveThisEpoch();
        }
        static void DeleteNode(void *node){
            MarkNodeGarbage(node);
        }
    };
}