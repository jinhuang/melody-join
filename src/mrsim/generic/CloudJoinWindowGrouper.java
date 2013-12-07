package mrsim.generic;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;







public class CloudJoinWindowGrouper  implements RawComparator<CloudJoinKey>, Configurable{

	private Configuration conf;
	
	@Override
	public int compare(CloudJoinKey key1, CloudJoinKey key2)
    {
		long 	part1		= key1.getPartitionID();
		long	win1		= key1.getWindowID();
		long	part2		= key2.getPartitionID();
		long	win2 		= key2.getWindowID();
		
		if( (win1==-1) && (win2==-1) )//p-p
		{
			if(part1==part2)
			{
				return 0;
			}
			else
			{
				return (part1<part2? -1:1);
			}
		}
		else if( (win1==-1) && (win2!=-1) ) //p-w
		{
			return -1;
		}
		else if ((win1!=-1) && (win2==-1)) //w-p
		{
			return 1;
		}
		else
		{
			
			
			long min1 = Math.min(part1, win1);
			long max1 = Math.max(part1, win1);
			long min2 = Math.min(part2, win2);
			long max2 = Math.max(part2, win2);
			
			if ( !((min1==min2) && (max1==max2)) )
			{
				if(min1==min2)
					return (max1<max2 ? -1:1);
				else
					return (min1<min2 ? -1:1);
			}
			else
			{
				long	prevPart1	= key1.getPrevItrPartion();
				long	prevPart2	= key2.getPrevItrPartion();
				//we should perform a check here and make sure we get values back
				long W = Long.parseLong(conf.get("W"));
				long V = Long.parseLong(conf.get("V"));
				
				if ( (part1==part2) && (prevPart1==prevPart2) )
					return 0;
				else if (part1==part2)
				{
					if (part1<win1)
					{
						if( (prevPart1==V) && (prevPart2==W) )
							return -1;
						else 
							return 1;
					}
					else
					{
						if( (prevPart1==V) && (prevPart2==W) )
							return 1;
						else
							return -1;
					}
				}
				else if (prevPart1==prevPart2)
				{
					if (prevPart1==V)
					{
						if (part1<win1)
							return -1;
						else
							return 1;
					}
					else
					{
						if (part1<win1)
							return 1;
						else
							return -1;
					}
				}
				else
					return 0;
			}
		}
		
 }// end compare


    public int compare(	byte[] b1, int s1, int l1, 
    						byte[] b2, int s2, int l2) 
	{
		
		//this is going to be needing some optimization but this is the easiest way to code this right now.
		
		long part1 = WritableComparator.readLong(b1, s1);
		long win1 = WritableComparator.readLong(b1, s1 + (Long.SIZE / 8));
		long part2 = WritableComparator.readLong(b2, s2);
		long win2 = WritableComparator.readLong(b2, s2 + (Long.SIZE / 8));
	
		if( (win1==-1) && (win2==-1) )//p-p
		{
			if(part1==part2)
			{
				return 0;
			}
			else
			{
				return (part1<part2? -1:1);
			}
		}
		else if( (win1==-1) && (win2!=-1) ) //p-w
		{
			return -1;
		}
		else if ((win1!=-1) && (win2==-1)) //w-p
		{
			return 1;
		}
		else
		{
			
			
			long min1 = Math.min(part1, win1);
			long max1 = Math.max(part1, win1);
			long min2 = Math.min(part2, win2);
			long max2 = Math.max(part2, win2);
			
			if ( !((min1==min2) && (max1==max2)) )
			{
				if(min1==min2)
					return (max1<max2 ? -1:1);
				else
					return (min1<min2 ? -1:1);
			}
			else
			{
				long	prevPart1	= WritableComparator.readLong(b1, s1 + (Long.SIZE + Long.SIZE) / 8);
				long	prevPart2	= WritableComparator.readLong(b2, s2 + (Long.SIZE + Long.SIZE) / 8);
				
				//we should perform a check here and make sure we get values back
				long W = Long.parseLong(conf.get("W"));
				long V = Long.parseLong(conf.get("V"));
				
				if ( (part1==part2) && (prevPart1==prevPart2) )
					return 0;
				else if (part1==part2)
				{
					if (part1<win1)
					{
						if( (prevPart1==V) && (prevPart2==W) )
							return -1;
						else 
							return 1;
					}
					else
					{
						if( (prevPart1==V) && (prevPart2==W) )
							return 1;
						else
							return -1;
					}
				}
				else if (prevPart1==prevPart2)
				{
					if (prevPart1==V)
					{
						if (part1<win1)
							return -1;
						else
							return 1;
					}
					else
					{
						if (part1<win1)
							return 1;
						else
							return -1;
					}
				}
				else
					return 0;
			}
		}
	}
    
    
    
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	

}
