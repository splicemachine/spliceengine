#!/bin/bash
echo "$* && ./sqlshell.sh && /bin/bash"
echo
 $* && ./sqlshell.sh && /bin/bash
