#!/usr/bin/python

import sys
import os
import re
import shutil

#Recursively update pom.xml. Replace envClassifier with platform
rootDir = '.'
platform = sys.argv[1]
for dirName, subdirList, fileList in os.walk(rootDir):
   if re.search(r'target', dirName, re.M|re.I):
      continue
   for fname in fileList:
      if fname != "pom.xml":
         continue
      fileName = dirName + '/' + fname
      pomName = dirName + '/' + "pom.tmp"
      fout = open(pomName, 'w')
      with open(fileName, 'r') as fin:
         curline = fin.readline() 
         fout.write(curline)
         for line in fin:
            preline = curline
            curline = line
            if re.search(r'-\${envClassifier}</artifactId>', curline, re.M|re.I) and re.search(r'<modelVersion>', preline, re.M|re.I):
               pattern = "-" + platform + "</artifactId>"
               line = re.sub("-\${envClassifier}</artifactId>", pattern, line)
               print line
            fout.write(line)
      fout.close()
      os.remove(fileName)
      shutil.move(pomName, fileName)
