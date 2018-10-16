import org.apache.hadoop.hive.ql.exec.UDF;

public class StringContain extends UDF {
	public Boolean evaluate(String regular,String line) {
		return line.contains(regular);
	}
}
