#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual
book_workspace=~/book-workspace/htdg-git

for ch in ch02 ch03 ch05 ch06 ch08 ch09 ch16 ch17 ch21
do
  # Usuwanie atrybutów id i language z listingów oraz dodawanie nowego wiersza przed </programlisting>
  sed '/<programlisting/s/ id="[^"]*"//; /<programlisting/s/ language="[^"]*"//; s|</programlisting>|\
</programlisting>|' $book_workspace/$ch.xml > /tmp/$ch.xml
  $bin/check_manuscript.py /tmp/$ch.xml $actual/$ch/*
done

# Test dla Avro
sed -e '/<programlisting/s/ id="[^"]*"//; /<programlisting/s/ language="[^"]*"//;  s|</programlisting>|\
</programlisting>|' $book_workspace/ch12.xml > /tmp/ch12.xml
$bin/check_manuscript.py /tmp/ch12.xml $actual/ch12/*

# Ogólny test
$bin/check_manuscript.py /tmp/ch08-mr-types.xml $actual/common/*
