#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
echo "./bin/go-ycsb load tikv -p tikv.pd="127.0.0.1:42379" -p tikv.type="raw" -p tikv.column_family="walfifo" -P workloads/workload_walfifo_test -p threadcount=256"
./bin/go-ycsb load tikv -p tikv.pd="127.0.0.1:42379" -p tikv.type="raw" -p tikv.column_family="walfifo" -P workloads/workload_walfifo_test -p threadcount=256
echo "./bin/go-ycsb load tikv -p tikv.pd="127.0.0.1:42379" -p tikv.type="raw" -p tikv.column_family="default" -P workloads/workload_walfifo_test -p threadcount=256"
./bin/go-ycsb load tikv -p tikv.pd="127.0.0.1:42379" -p tikv.type="raw" -p tikv.column_family="default" -P workloads/workload_walfifo_test -p threadcount=256
