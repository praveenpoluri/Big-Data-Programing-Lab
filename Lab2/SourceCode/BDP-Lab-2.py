#!/usr/bin/env python
# coding: utf-8

# In[1]:


from graphframes import *
from pyspark import *
from pyspark.sql import *


# In[2]:


import findspark


# In[3]:


sc=SparkContext().getOrCreate()


# In[ ]:





# In[ ]:





# In[5]:


sqlContext = SQLContext(sc)


# In[6]:


df_wordgame = sqlContext.read.format("csv").option("header", "true").csv('wordgame_20170721.csv')


# In[7]:


df_wordgame.registerTempTable("wordgame")


# In[8]:


df_wordgame.show()


# In[9]:


after_duplicate=sqlContext.sql('select author,word1 as id,word2,source,sourceID from wordgame group by  author,word1,word2,source,sourceID limit 1000')
after_duplicate.registerTempTable("newwordgame")


# In[10]:



vertices=sqlContext.sql('select author,id,word2,source,sourceID from newwordgame group by  author,id,word2,source,sourceID')


# In[11]:


edges=sqlContext.sql('select id as src,word2 as dst, source as source from newwordgame group by id,word2,source')


# In[12]:


from graphframes import *
graph=GraphFrame(vertices,edges)


# In[13]:


pageRank=graph.pageRank(resetProbability=0.15,maxIter=1)


# In[14]:


pageRank.vertices.show()


# In[15]:



pageRank.edges.show()


# In[ ]:




