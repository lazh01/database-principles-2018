package bufmgr;
        /**
         * 
         * @author Sebastian Larsen
         * 
         * The clock replacement policy looks at the frametable as a clock
         * and loops through the indices evauluating them based on their state.
         * 
         * The state of an index can be one of the following 3
         * 0 = available : the index can be replaced
         * 1 = pinned : the index cannot be replaced
         * 2 = prevpinned : should be set to available instead of being replaced when picked.
         */
public class Clock extends Replacer{
        
        private int current;

	protected Clock(BufMgr bufmgr) {
		super(bufmgr);
		current = -1;
	}

	@Override
	public void newPage(FrameDesc fdesc) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void freePage(FrameDesc fdesc) {
            
            fdesc.state=0;
		
	}

	@Override
	public void pinPage(FrameDesc fdesc) {
		fdesc.state=1;
		
	}

	@Override
	public void unpinPage(FrameDesc fdesc) {
		if(fdesc.pincnt == 0){
                    fdesc.state=2;
                }
		
	}

	@Override
        /**
         * 
         * pickVictim tries to pick a frame which can be replaced.
         * Loops through the frametable upto 2 times looking for a frame to be replaced.
         * For each frame it reaches it checks if its state is available(0), in which the index is returned,
         * or if the state is prevpinned(2), in which case it is set to available.
         * If no index is found in two cycles through the frametable, it means
         * that every frame is unavailable, and -1 is returned
         * 
         * 
         */
	public int pickVictim() {
            int count = 0;
            while (count<frametab.length*2){
                current = (current + 1) % frametab.length;
                
                if (frametab[current].state == 0){
                    return current;
                } else if (frametab[current].state == 2) {
                    frametab[current].state = 0;
                }
                count++;
            }
            return -1;
        }
}
