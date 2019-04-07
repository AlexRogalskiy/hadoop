#!/usr/bin/env bash

# Format NLineInputFormat zwraca pojedyncze wiersze - 
# klucz to pozycja, a wartość to identyfikator URI z usługi S3
read offset s3file

# Pobieranie pliku z usługi S3 na dysk lokalny
echo "reporter:status:Pobieranie $s3file" >&2
$HADOOP_INSTALL/bin/hadoop fs -get $s3file .

# Wypakowywanie lokalnego pliku z formatów bzip i tar
target=`basename $s3file .tar.bz2`
mkdir -p $target
echo "reporter:status:Wypakowywanie (tar) $s3file do $target" >&2
tar jxf `basename $s3file` -C $target

# Wypakowywanie plików dla stacji z formatu gzip i złączanie ich w jeden plik
echo "reporter:status:Wypakowywanie (gzip) $target" >&2
for file in $target/*/*
do
  gunzip -c $file >> $target.all
  echo "reporter:status:Przetworzono plik $file" >&2
done

# Umieszczanie pliku w formacie gzip w systemie HDFS
echo "reporter:status:Pakowanie (gzip) $target i umieszczanie w HDFS" >&2
gzip -c $target.all | $HADOOP_INSTALL/bin/hadoop fs -put - gz/$target.gz