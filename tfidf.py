"""refer to https://www.youtube.com/watch?v=hXNbFNCgPfY"""
import jieba
import os
import sys
import pandas as pd
import math
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import matplotlib.font_manager as font_manager

font_dirs = ['./', ]
font_files = font_manager.findSystemFonts(fontpaths=font_dirs)
font_list = font_manager.createFontList(font_files)
font_manager.fontManager.ttflist.extend(font_list)
print(font_list)
plt.rcParams['font.family'] = 'DFKai-SB'

docA = []
docB = []
with open("docA.txt","r", encoding='utf8') as s:
    content = s.read()
    seg_list = jieba.cut(content, cut_all=False)
    for i in seg_list:
        docA.append(i)
        
with open("docB.txt","r", encoding='utf8') as s:
    content = s.read()
    seg_list = jieba.cut(content, cut_all=False)
    for i in seg_list:
        docB.append(i)

wordSet =  set(docA).union(set(docB))

"""The fromkeys() method creates a new dictionary from the given sequence of elements with a value provided by the user."""
dictA = dict.fromkeys(wordSet,0)
dictB = dict.fromkeys(wordSet,0)

for word in docA:
    dictA[word] = dictA[word]+1

for word in docB:
    dictB[word] = dictB[word]+1

result = pd.DataFrame([dictA,dictB])

def computeTF(wordDict,bow):
    tfDict = {}
    bowCount = len(bow)
    for word, count in wordDict.items():
        tfDict[word] = count / float(bowCount)
    return tfDict

tfBowA = computeTF(dictA,docA)
tfBowB = computeTF(dictB,docB)

def computeIDF(doclist):
    idfDict = {}
    N = len(doclist)
    
    idfDict = dict.fromkeys(doclist[0].keys(),0)
    for doc in doclist:
        for word, val in doc.items():
            if val > 0:
                idfDict[word] = idfDict[word] + 1
                
    for word,val in idfDict.items():
        idfDict[word] =  math.log(N / float(val))
        
    return idfDict
    

idfs = computeIDF([dictA,dictB])

def computeTFIDF(tfBow, idfs):
    tfidf = {}
    for word,val in tfBow.items():
        tfidf[word] = val * idfs[word]
    return tfidf



tfidfBowA = computeTFIDF(tfBowA,idfs)
tfidfBowB = computeTFIDF(tfBowB,idfs)

df = pd.DataFrame([tfidfBowA,tfidfBowB],index=list("AB"))


df = df.sort_values(by='B', axis=1, ascending=False)

dfa = df.iloc[:,0:10].T
print(dfa.index.values)
plt.figure()

ax = dfa.plot.bar()
ax.set_xlabel("文字")
print(dfa.columns.values)
