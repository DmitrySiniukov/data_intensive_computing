curl -XPUT http://localhost:9200/sentiments -d '
{  
   "mappings":{  
      "tweet":{  
         "properties":{  
            "created_at":{  
               "type":"date"
            },
            "text":{  
               "type":"string"
            },
            "sentiment":{  
               "type":"keyword"
            },
            "sentiment_int":{  
               "type":"integer"
            },
            "price":{  
               "type":"double"
            }
         }
      }
   }
}'

