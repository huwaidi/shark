package shark.udf;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * 
 * @author Hussein Huwaidi
 * @version 0.1
 * 
 * This is a Shark UDAF that calculates the maximum value of a given argument.
 */

public class Max extends UDAF {
	public static class MaximumDoubleUDAFEvaluator implements UDAFEvaluator {
		private DoubleWritable result;
		
		/**
		 * Called by Hive to initialize an instance of the UDAF evaluator class.
		 */
		public void init() {
			result = null;
		}

		/** 
		 * Process a new row of data into the aggregation buffer.
		 * @param value The argument/column passed to the UDF
		 * @return True if successful, false otherwise
		 */
		public boolean iterate(DoubleWritable value) {
			// ignore value if null
			if (value == null) {
				return true;
			}
			
			// if there is no maximum set yet, just take current value, else take maximum
			if (result == null) {
				result = new DoubleWritable(value.get());
			} else {
				result.set(Math.max(result.get(), value.get()));
			}
			return true;
		}

		/**
		 * Return the contents of the current aggregation in a persistable way.
		 * Here, persistable means the return value can only be built up in terms
		 * of Java primitives, arrays, primitive wrappers (e.g., Double), Hadoop Writables,
		 * Lists, and Maps. Do NOT use your own classes (even if they implement
		 * java.io.Serializable).
		 * @return
		 */
		public DoubleWritable terminatePartial() {
			return result;
		}

		/**
		 * Merge a partial aggregation returned by terminatePartial into the current aggregation. 
		 * @param other
		 * @return
		 */
		public boolean merge(DoubleWritable other) {
			return iterate(other);
		}

		/**
		 * Return the final result of the aggregation to Hive.
		 * @return
		 */
		public DoubleWritable terminate() {
			return result;
		}
	}
}