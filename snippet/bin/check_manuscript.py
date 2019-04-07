#!/usr/bin/env python

# Sprawdzanie, czy oczekiwane (lub rzeczywiste) fragmenty znajdują się w manuskrypcie. Na przykład:
# bin/check_manuscript.py  ~/book-workspace/htdg-git/ch16-pig.xml expected/ch16-pig/grunt/*

import sys

manuscript = open(sys.argv[1], 'r').read()

for snippet_file in sys.argv[2:]:
  lines = open(snippet_file, 'r').readlines()
  if lines[0].startswith("<!--"):
    doc = "".join(lines[1:]) # Usuwanie pierwszego wiersza, jeśli jest komentarzem
  else:
    doc = "".join(lines[0:])
  snippet = doc.strip()
  index = manuscript.find(snippet)
  if index == -1:
    print "Fragmentu nie znaleziono", snippet_file
  #else:
  #  print "Fragment znaleziono", snippet_file

