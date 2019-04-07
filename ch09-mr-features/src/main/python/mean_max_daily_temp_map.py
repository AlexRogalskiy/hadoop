#!/usr/bin/env python

import sys

# Modyfikacja daty na postać z miesiącem i dniem
for line in sys.stdin:
  (station, date, temp) = line.strip().split("\t")
  print "%s\t%s\t%s" % (station, date[4:8], temp) 