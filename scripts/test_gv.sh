#!/bin/bash
NODE=$1

kubectl run gv --rm -it --restart=Never --image=busybox \
  --overrides='{"spec":{"runtimeClassName":"gvisor","nodeName":"'"$NODE"'"}}' \
  -- dmesg

