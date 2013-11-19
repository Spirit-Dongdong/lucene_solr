package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The {@link CountLimitingCollector} is used to limit the doc size collected by the search thread. When 
 * number of collected docs reaches maximum size, the search thread is stopped by throwing a 
 * {@link CountLimitingException}
 */
public class CountLimitingCollector extends Collector {
  private int maxSize;
  private int count;
  private int lastDoc;
  
  private Collector collector;
  
  private int docBase;
  
  protected boolean outOfOrder;
  protected boolean shouldScore;
  
  public CountLimitingCollector(int maxSize, Collector collector,
      boolean outOfOrder, boolean shouldScore) {
    super();
    this.maxSize = maxSize;
    this.collector = collector;
    this.outOfOrder = outOfOrder;
    this.shouldScore = shouldScore;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    collector.setScorer(scorer);
  }
  
  @Override
  public void collect(int doc) throws IOException {
    count++;
    if (count > maxSize) {
      count--;
      lastDoc = docBase + doc;
      throw new CountLimitingException(maxSize, count, lastDoc);
      
    }
    collector.collect(doc);
  }
  
  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    this.docBase = context.docBase;
    collector.setNextReader(context);
  }
  
  @Override
  public boolean acceptsDocsOutOfOrder() {
    return collector.acceptsDocsOutOfOrder();
  }
  
  public static class CountLimitingException extends RuntimeException {
    private int maxSize;
    private int count;
    private int lastDoc;
    
    public CountLimitingException(int maxSize, int count, int lastDoc) {
      super();
      this.maxSize = maxSize;
      this.count = count;
      this.lastDoc = lastDoc;
    }

    public int getMaxSize() {
      return maxSize;
    }

    public int getCount() {
      return count;
    }

    public int getLastDoc() {
      return lastDoc;
    }
    
  }
  
}
