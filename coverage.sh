#!/usr/bin/env bash

# This file is part of asyncio-taskpool.

# asyncio-taskpool is free software: you can redistribute it and/or modify it under the terms of
# version 3.0 of the GNU Lesser General Public License as published by the Free Software Foundation.

# asyncio-taskpool is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License along with asyncio-taskpool.
# If not, see <https://www.gnu.org/licenses/>.

coverage erase
coverage run 2> /dev/null

typeset report=$(coverage report)
typeset total=$(echo "${report}" | awk '$1 == "TOTAL" {print $NF; exit}')

if [[ ${total} == 100% ]]; then
  echo ${total}
else
  echo "${report}"
fi
