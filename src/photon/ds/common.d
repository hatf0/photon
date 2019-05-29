module photon.ds.common;

ref T unshared(T)(ref shared  T value) 
if (is(T == class)) {
     return *cast(T*)&value;
}

ref T* unshared(T)(ref shared(T)* value) {
     return *cast(T**)&value;
}

ref T unsharedRef(T)(ref shared(T) value) {
     return *cast(T*)&value;
}

interface WorkQueue(T) {
    shared void push(T item);
    shared T pop(); // blocks if empty
    shared bool tryPop(ref T item); // non-blocking
}

// intrusive list helper
T removeFromList(T)(T head, T item) {
	if (head is item) return head.next;
	else {
		auto p = head;
		while(p.next) {
			if (p.next is item) {
				p.next = item.next;
				break;
			}
			else 
				p = p.next;
		}
		return head;
	}
}