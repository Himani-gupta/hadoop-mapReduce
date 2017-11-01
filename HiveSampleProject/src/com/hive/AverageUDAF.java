package com.hive;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

 
public class AverageUDAF extends UDAF{

    static final Log LOG = LogFactory.getLog(AverageUDAF.class.getName());

 

    public static class AverageUDAFEvaluator implements UDAFEvaluator{

        public static class Item{

            double total = 0;

            int count = 0;

        }      

        private Item item = null;

        public AverageUDAFEvaluator(){
            super();
            init();
        }


        public void init() {
            item = new Item();          
        }

        public boolean iterate(double value) throws HiveException{

            if(item == null)

                throw new HiveException("Item is not initialized");

            item.total = item.total + value;

            item.count = item.count + 1;

            return true;
        }

        public double terminate(){
        	
            return item.total/item.count;
        }


        public Item terminatePartial(){         

            return item;

        }



        public boolean merge(Item another){        

            if(another == null) return true;

            item.total += another.total;

            item.count += another.count;

            return true;

        }

    }

}
