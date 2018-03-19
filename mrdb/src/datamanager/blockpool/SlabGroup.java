package datamanager.blockpool;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class SlabGroup {
	
	final List<Slab> slabList;
	
	SlabGroup() {
		slabList = new LinkedList<Slab>();
	}
	
	void remove(Slab slab) {
		slabList.remove(slab);
	}
	
	void add(Slab slab) {
		slabList.add(slab);
	}
	
	Iterator<Slab> iterator() {
		return slabList.iterator();
	}
}
