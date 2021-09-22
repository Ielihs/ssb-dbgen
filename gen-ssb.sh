#!/bin/bash

set -e

BIN_PATH=`dirname "$0"`
BIN_PATH=`cd "${BIN_PATH}"; pwd`
TABLE_DIR=$BIN_PATH
SIZE=100
CSV_OUTPUT_FORMAT="OFF"
YMD_DASH_DATE="OFF"
FORKS=`cat /proc/cpuinfo | grep "processor" | wc -l`

usage() {
echo "
Usage: $0 <options>
  Optional options:
     -h          Show usage
     -f <n>      Set number of parallel processes , default: <cpu cores>
     -o <dir>    Set output path for generated datasets
     -s <n>      Set Scale Factor , default: 100g
     -c          Adhere to the CSV format for the output, i.e. use commans (,) as a field separator,and enclose strings in double-quotes to ensure any commas within them aren't mis-interpreted.
     -d          Generates dates to dashes between fields, i.e. YYYY-MM-DD
  Eg.
    $0 -h           show usage
    $0 -s 10 -f 2   example
"
}

parseArgs() {
  while getopts "hs:f:o:cd" opt; do
    case ${opt} in
    s) SIZE=$OPTARG ;;
    f) FORKS=$OPTARG ;;
    c) CSV_OUTPUT_FORMAT="ON";;
    d) YMD_DASH_DATE="ON";;
    o) 
      TABLE_DIR=$OPTARG
      export DSS_PATH=${TABLE_DIR}
      if [ ! -d ${TABLE_DIR} ]; then
        mkdir -p ${TABLE_DIR}
      fi
      ;;
    h) usage; exit 0 ;;
    :) usage; exit 1 ;;
    ?) usage; exit 1 ;;
    esac
  done
}

init() {
  if ! [ -x "$(command -v cmake3)" ]; then
      yum install -y cmake3 
  fi

  rm -rf $BIN_PATH/build && mkdir -p $BIN_PATH/build
  cmake3 -DYMD_DASH_DATE=$1 -DCSV_OUTPUT_FORMAT=$2 -DEOL_HANDLING=$2 ${BIN_PATH} -B${BIN_PATH}/build 
  cp -f ${BIN_PATH}/build/src/config.h ${BIN_PATH}/src/config.h
  cmake3 --build ${BIN_PATH}/build
 
  export DSS_CONFIG=${BIN_PATH}
  #command -v ${BIN_PATH}/build/dbgen >/dev/null 2>&1
}

generateTabel() {
  table_name=$1
  thread_num=$2
 
  if [ $thread_num -eq "1" ] || [ $SIZE -eq "1" ]; then
    echo "-   start to generate ${table_name}.tbl"
    ${BIN_PATH}/build/dbgen -s $SIZE -T ${table_name:0:1} > /dev/null 2>&1
    echo "- success to generate ${table_name}.tbl"
    return 0
  fi

  tmp_fifofile="/tmp/$$.fifo"
  mkfifo $tmp_fifofile
  exec 6<>$tmp_fifofile
  rm $tmp_fifofile

  for ((i=0;i<${thread_num};i++)); do
    echo
  done >&6
  
  for i in `seq 1 $SIZE `; do
    read -u6
    {
      echo "-   start to generate ${table_name}.tbl.${i}"
      ${BIN_PATH}/build/dbgen -s $SIZE -T ${table_name:0:1} -C $SIZE -S $i > /dev/null 2>&1
      echo "- success to generate ${table_name}.tbl.${i}" 
      echo >&6
    } &
  done

  wait
  exec 6>&-
}

parseArgs "$@"

echo -n "Preparing ... "
init $YMD_DASH_DATE $CSV_OUTPUT_FORMAT >/dev/null 2>&1
echo "done"

echo "Generating new data set of scale factor $SIZE"
echo "Generate path: $TABLE_DIR"  
echo "Number of processer is $FORKS"

generateTabel lineorder $FORKS
generateTabel cutomers 1 &
generateTabel date 1 & 
generateTabel parts 1 &
generateTabel suppliers 1 &

wait
echo "Data generation completed!"

du -sh ${TABLE_DIR}/*.tbl*
