package mrsim.generic;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;







public class CloudJoinBaseGrouper  implements RawComparator<CloudJoinKey> {

	
	@Override
	public int compare(CloudJoinKey key1, CloudJoinKey key2)
    {
		
		long 	part1	= key1.getPartitionID();
		long	win1	= key1.getWindowID();
		long	part2	= key2.getPartitionID();
		long	win2 	= key2.getWindowID();
		if (win1==-1 && win2==-1)
		{//p-p
			if(part1==part2) return 0;
			else return (part1 < part2 ? -1 : 1);
		}
		else if (win1==-1)
		{ //p-w
			return -1;
		}
		else if (win2==-1)
		{ //w-p
			return 1;	
		}
		else
		{//w-w
			long minOfo1 = Math.min(part1, win1);
			long maxOfo1 = Math.max(part1, win1);
			
			long minOfo2 = Math.min(part2, win2);
			long maxOfo2 = Math.max(part2, win2);
			
			if (minOfo1==minOfo2)
			{
				if(maxOfo1==maxOfo2)
					return 0;
				else
					return (maxOfo1 < maxOfo2 ? -1:1);
			}
			else
				return (minOfo1 < minOfo2 ? -1:1);
		}
 }

	@Override
    public int compare(	byte[] b1, int s1, int l1, 
    						byte[] b2, int s2, int l2) 
	{
		
		long part1 = WritableComparator.readLong(b1, s1);
		long win1 = WritableComparator.readLong(b1, s1 + (Long.SIZE / 8));
		long part2 = WritableComparator.readLong(b2, s2);
		long win2 = WritableComparator.readLong(b2, s2 + (Long.SIZE / 8));
		
		
		if (win1==-1 && win2==-1)
		{//p-p
			if(part1==part2) return 0;
			else return (part1 < part2 ? -1 : 1);
		}
		else if (win1==-1)
		{ //p-w
			return -1;
		}
		else if (win2==-1)
		{ //w-p
			return 1;	
		}
		else
		{//w-w
			long minOfo1 = Math.min(part1, win1); //System.out.println("CJBG_BS: minOfo1:" + minOfo1);
			long maxOfo1 = Math.max(part1, win1); //System.out.println("CJBG_BS: maxOfo1:" + maxOfo1);
			
			long minOfo2 = Math.min(part2, win2); //System.out.println("CJBG_BS: minOfo2:" + minOfo2);
			long maxOfo2 = Math.max(part2, win2); //System.out.println("CJBG_BS: maxOfo2:" + maxOfo2);
			
			if (minOfo1==minOfo2)
			{
				if(maxOfo1==maxOfo2)
					return 0;
				else
					return (maxOfo1 < maxOfo2 ? -1:1);
			}
			else
				return (minOfo1 < minOfo2 ? -1:1);
		}
    }
	

}
