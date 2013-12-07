package mrsim.generic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.iojin.melody.utils.DistanceUtil;
import com.iojin.melody.utils.HistUtil;


public class VectorElemHD implements WritableComparable<VectorElemHD>{

	private long partitionID = -1;
	private long windowID = -1;
	private long key;	
	private List<Double> elemList;
	private long prevPartition = -1;

	public VectorElemHD() {

	}



	// this could be an abstract method in an abstract class in,
	// take a text type and populate the values based off the input
	public VectorElemHD(Text text) {		
		StringTokenizer line = new StringTokenizer(text.toString());
		elemList = new ArrayList<Double>();
		this.key = Long.parseLong(line.nextToken());
		while(line.hasMoreElements()){
			elemList.add(Double.parseDouble(line.nextToken()));
		}	
		normalize();
	}

	public VectorElemHD(String text) {		
		StringTokenizer line = new StringTokenizer(text);
		elemList = new ArrayList<Double>();
		this.key = Long.parseLong(line.nextToken());
		while(line.hasMoreElements()){
			elemList.add(Double.parseDouble(line.nextToken()));
		}
		normalize();
	}
	
	private void normalize() {
		double[] array = new double[elemList.size()];
		for (int i = 0 ; i < array.length; i++) {
			array[i] = elemList.get(i);
		}
		array = HistUtil.normalizeArray(array);
		for (int i = 0; i < array.length; i++) {
			elemList.set(i, array[i]);
		}		
	}

	public int getSizeInBytes() {
		return ((Long.SIZE + // partitionID
				Long.SIZE + // windowID
				Long.SIZE + // key
				((Double.SIZE) * 9) + // elems
		Long.SIZE // prevPartition
		) / 8);

	}

	public long getKey() {
		return key;
	}

	public void setPartitionID(long partitionID) {
		this.partitionID = partitionID;
	}

	public long getPartitionID() {
		return partitionID;
	}

	public long getWindowID() {
		return windowID;
	}

	public void setWindowID(long windowID) {
		this.windowID = windowID;
	}

	public long getPrevPartition() {
		return prevPartition;
	}

	public void setPrevPartition(long prevPartition) {
		this.prevPartition = prevPartition;
	}

	

	// would need to be implemented in an abstract class
	public double getDistanceBetween(VectorElemHD o) {
		// double deltaX = this.x - o.getX();
		// double deltaY = this.y - o.getY();
		
		double listLength = this.getElemList().size()<o.getElemList().size()?this.getElemList().size():o.getElemList().size();
		double interValue=0;
		for(int i=0; i<listLength;i++){
			interValue += Math.pow((this.getElemList().get(i)-o.getElemList().get(i)),2);
		}
		
		return Math.sqrt(interValue);
	}
	
	public double getDistanceByEmd(VectorElemHD o,double[] bins,int dimension) {
		// double deltaX = this.x - o.getX();
		// double deltaY = this.y - o.getY();
		double emd =0;
		int listLength = this.getElemList().size()>o.getElemList().size()?this.getElemList().size():o.getElemList().size();
		double [] hist1 = new double[listLength];
		double [] hist2 = new double[listLength];
		hist1=list2array(this.getElemList());
		hist2=list2array(o.getElemList());
//		System.out.println("Compute " + FormatUtil.toString(hist1) + " : " + FormatUtil.toString(hist2));
		emd = DistanceUtil.getEmdLTwo(hist1, hist2, dimension, bins);
//		System.out.println(emd);
		
//		emd = getDistanceBetween(o);
		
		
		return emd;
	}

	public List<Double> getElemList() {
		return elemList;
	}
	public void setElemList(List<Double> elemList) {
		this.elemList = elemList;
	}



	@Override
	public void readFields(DataInput in) throws IOException {
		this.partitionID = in.readLong();
		this.windowID = in.readLong();
		this.key = in.readLong();		
		this.prevPartition = in.readLong();		
		int elemSize = in.readInt();
		elemList = new ArrayList<Double>();
		while(elemSize>0){
			double d = in.readDouble();
			elemList.add(d);
			elemSize--;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(partitionID);
		out.writeLong(windowID);
		out.writeLong(key);
		out.writeLong(prevPartition);
		out.writeInt(elemList.size());
		for(Double d:elemList)
			out.writeDouble(d);
		
	}

	@Override
	public int compareTo(VectorElemHD o) {
		if(this.getElemList().size()!=o.getElemList().size())
			return this.getElemList().size()<o.getElemList().size()?-1:1;
		int reVal=0;
		for(int i=0;i<this.getElemList().size();i++){
			if(this.getElemList().get(i)!=o.getElemList().get(i)){
				reVal = this.getElemList().get(i)<o.getElemList().get(i)?-1:1;
				break;
			}
		}
		return reVal;
	}

	// must be used for grouper, abstract class
	public int compareToGrouper(VectorElemHD o) {
		if (this.key != o.getKey())
			return this.key < o.getKey() ? -1 : 1;
		else
			return 0;
	}

	@Override
	public String toString(){
//		String str="";
//		for(Double d: this.getElemList()){
//			str += " " +d;
//		}		
		return key+"";
	}
	
	public String toStringAll() {
		String str="";
		for(Double d: this.getElemList()){
			str += " " +d;
		}		
		return (key +str+ "   partitionID" + partitionID+ " prevPartition " + prevPartition);
	}

	public String toStringPart() {
		String str="";
		for(Double d: this.getElemList()){
			str = str+ " " +d;
		}		
		return (key +str+ " " + partitionID);
	}

	public String toStringPrev() {
		String str="";
		for(Double d: this.getElemList()){
			str = str+ " " +d;
		}		
		return (key +str+ " " + prevPartition);
	}
	
	public double [] list2array(List<Double> list){
		double[] hist = new double[list.size()];
		int i=0;
		for(Double d:list)
			hist[i++]=d;
		return hist;
	}



	public void setKey(long key) {
		this.key = key;
	}
	
}
