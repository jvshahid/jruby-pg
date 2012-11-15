#!/usr/bin/env bash

rake                            # test without ssl
PG_TEST_SLL=1 rake              # test with ssl
