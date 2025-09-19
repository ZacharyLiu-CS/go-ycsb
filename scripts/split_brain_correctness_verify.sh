#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
./bin/go-ycsb load tikv -p tikv.pd="127.0.0.1:42379" -p tikv.type="raw" -p tikv.auth_mode=true -P workloads/workload_correctness_verify -p threadcount=64
./bin/go-ycsb  run tikv -p tikv.pd="127.0.0.1:42379" -p tikv.type="raw" -p tikv.auth_mode=true -P workloads/workload_correctness_verify -p threadcount=64