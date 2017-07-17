for i in `ls query*`;do echo $i && impala-shell -f $i;done
