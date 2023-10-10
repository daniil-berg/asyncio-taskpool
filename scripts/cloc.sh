#!/usr/bin/env bash

typeset option
if getopts 'bcmp' option; then
  if [[ ${option} == [bcmp] ]]; then
    shift
  else
    echo >&2 "Invalid option '$1' provided"
    exit 1
  fi
fi

typeset source=$1
if [[ -z ${source} ]]; then
  echo >&2 Source file/directory missing
  exit 1
fi

typeset blank code comment commentpercent
read blank comment code commentpercent < <( \
  cloc --csv --quiet --hide-rate --include-lang Python ${source} |
  awk -F, '$2 == "SUM" {printf ("%d %d %d %1.0f", $3, $4, $5, 100 * $4 / ($5 + $4)); exit}'
)

case ${option} in
  b) echo ${blank} ;;
  c) echo ${code} ;;
  m) echo ${comment} ;;
  p) echo ${commentpercent} ;;
  *) echo Blank lines: ${blank}
     echo Lines of comments: ${comment}
     echo Lines of code: ${code}
     echo Comment percentage: ${commentpercent} ;;
esac
