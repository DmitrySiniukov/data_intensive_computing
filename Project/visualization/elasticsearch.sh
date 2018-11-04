curl -XPUT http://localhost:9200/twitter -d '
{  
   "mappings":{  
      "tweet":{  
         "properties":{  
            "created_at":{  
               "type":"date"
            },
            "sentiment":{  
               "type":"string"
            },
            "text":{  
               "type":"string"
            }
         }
      }
   }
}'