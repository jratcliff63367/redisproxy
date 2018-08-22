#pragma once

#include <assert.h>

namespace objectpool
{

template < typename PoolType >
class ObjectPool
{
private:
	class ObjectPoolNode
	{
	public:
		PoolType object;
		ObjectPoolNode* previous;
		ObjectPoolNode* next;
	};

	class ObjectPoolChunk
	{
	public:
		ObjectPoolNode* chunk;
		ObjectPoolChunk* next;
	};

public:
	/**
		Iterator objects are used for safe traversal of the allocated
		members of a pool.
	 */
	class Iterator
	{
		friend class ObjectPool< PoolType >;

	public :
		/// Increments the iterator to reference the next node in the
		/// linked list. It is an error to call this function if the
		/// node this iterator references is invalid.
		inline void operator++()
		{
			assert(node != NULL);
			node = node->next;
		}
		/// Returns true if it is OK to deference or increment this
		/// iterator.
		inline operator bool()
		{
			return (node != NULL);
		}

		/// Returns the object referenced by the iterator's current
		/// node.
		inline PoolType& operator*()
		{
			return node->object;
		}
		/// Returns a pointer to the object referenced by the
		/// iterator's current node.
		inline PoolType* operator->()
		{
			return &node->object;
		}

	private:
		// Constructs an iterator referencing the given node.
		inline Iterator(ObjectPoolNode* node)
		{
			this->node = node;
		}

		ObjectPoolNode* node;
	};

	ObjectPool(int chunk_size = 0, bool grow = false);
	~ObjectPool();

	/// Initialises the pool to a given size.
	void Initialise(int chunk_size, bool grow = false);

	/// Returns the head of the linked list of allocated objects.
	inline Iterator Begin();

	/// Attempts to allocate a deallocated object in the memory pool. If
	/// the process is successful, the newly allocated object is returned.
	/// If the process fails (due to no free objects being available), NULL
	/// is returned.
	inline PoolType* AllocateObject();

	/// Deallocates the object pointed to by the given iterator.
	inline void DeallocateObject(Iterator& iterator);
	/// Deallocates the given object.
	inline void DeallocateObject(PoolType* object);

    /// return unused memory to the heap
    void ReclaimMemory();

    /// Returns the number of objects in the pool.
	inline int GetSize() const;
	/// Returns the number of object chunks in the pool.
	inline int GetNumChunks() const;
	/// Returns the number of allocated objects in the pool.
	inline int GetNumAllocatedObjects() const;

private:
	// Creates a new pool chunk and appends its nodes to the beginning of the free list.
	void CreateChunk();

	int chunk_size;
	bool grow;

	ObjectPoolChunk* pool;

	// The heads of the two linked lists.
	ObjectPoolNode* first_allocated_node;
	ObjectPoolNode* first_free_node;

	int num_allocated_objects;
};


template < typename PoolType >
ObjectPool< PoolType >::ObjectPool(int _chunk_size, bool _grow)
{
	chunk_size = 0;
	grow = _grow;

	num_allocated_objects = 0;

	pool = NULL;
	first_allocated_node = NULL;
	first_free_node = NULL;

	if (_chunk_size > 0)
		Initialise(_chunk_size, _grow);
}

template < typename PoolType >
ObjectPool< PoolType >::~ObjectPool()
{
    ReclaimMemory();
}

// Initialises the pool to a given size.
template < typename PoolType >
void ObjectPool< PoolType >::Initialise(int _chunk_size, bool _grow)
{
	// Should resize the pool here ... ?
	if (chunk_size > 0)
		return;

	if (_chunk_size <= 0)
		return;

	grow = _grow;
	chunk_size = _chunk_size;
	pool = NULL;

	// Create the initial chunk.
	// TSC - no, delay allocation until something is active, and elsewhere
    //  immediately return memory when no longer active
    //CreateChunk(); 
}

// Returns the head of the linked list of allocated objects.
template < typename PoolType >
typename ObjectPool< PoolType >::Iterator ObjectPool< PoolType >::Begin()
{
	return typename ObjectPool< PoolType >::Iterator(first_allocated_node);
}

// Attempts to allocate a deallocated object in the memory pool.
template < typename PoolType >
PoolType* ObjectPool< PoolType >::AllocateObject()
{
	// We can't allocate a new object if the deallocated list is empty.
	if (first_free_node == NULL)
	{
		// Attempt to grow the pool first.
		if (grow)
		{
			CreateChunk();
			if (first_free_node == NULL)
				return NULL;
		}
		else
			return NULL;
	}

	// We're about to allocate an object.
	++num_allocated_objects;

	// This one!
	ObjectPoolNode* allocated_object = first_free_node;

	// Remove the newly allocated object from the list of deallocated objects.
	first_free_node = first_free_node->next;
	if (first_free_node != NULL)
		first_free_node->previous = NULL;

	// Add the newly allocated object to the head of the list of allocated objects.
	if (first_allocated_node != NULL)
	{
		allocated_object->previous = NULL;
		allocated_object->next = first_allocated_node;
		first_allocated_node->previous = allocated_object;
	}
	else
	{
		// This object is the only allocated object.
		allocated_object->previous = NULL;
		allocated_object->next = NULL;
	}

	first_allocated_node = allocated_object;

	return new (&allocated_object->object) PoolType();
}

// Deallocates the object pointed to by the given iterator.
template < typename PoolType >
void ObjectPool< PoolType >::DeallocateObject(Iterator& iterator)
{
	// We're about to deallocate an object.
	--num_allocated_objects;

	ObjectPoolNode* object = iterator.node;
	object->object.~PoolType();

	// Get the previous and next pointers now, because they will be overwritten
	// before we're finished.
	ObjectPoolNode* previous_object = object->previous;
	ObjectPoolNode* next_object = object->next;

	if (previous_object != NULL)
		previous_object->next = next_object;
	else
	{
		assert(first_allocated_node == object);
		first_allocated_node = next_object;
	}

	if (next_object != NULL)
		next_object->previous = previous_object;

	// Insert the freed node at the beginning of the free object list.
	if (first_free_node == NULL)
	{
		object->previous = NULL;
		object->next = NULL;
	}
	else
	{
		object->previous = NULL;
		object->next = first_free_node;
	}

	first_free_node = object;

	// Increment the iterator, so it points to the next active object.
	iterator.node = next_object;

    if (num_allocated_objects == 0)
    {
        ReclaimMemory();
    }
}

// Deallocates the given object.
template < typename PoolType >
void ObjectPool< PoolType >::DeallocateObject(PoolType* object)
{
	// This assumes the object has the same address as the node, which will be
	// true as long as the struct definition does not change.
	Iterator iterator((ObjectPoolNode*) object);
	DeallocateObject(iterator);
}

// Returns unused memory to the heap
//  only works if nothing allocated, as allocations may 
//  span many chunks
template < typename PoolType >
void ObjectPool< PoolType >::ReclaimMemory()
{
    assert(num_allocated_objects == 0);

    // no more free nodes when we're done
    first_free_node = 0;

    while (pool)
    {
        ObjectPoolChunk* next_chunk = pool->next;

        delete[] pool->chunk;
        delete pool;

        pool = next_chunk;
    }
}



// Returns the number of objects in the pool.
template < typename PoolType >
int ObjectPool< PoolType >::GetSize() const
{
	return chunk_size * GetNumChunks();
}

/// Returns the number of object chunks in the pool.
template < typename PoolType >
int ObjectPool< PoolType >::GetNumChunks() const
{
	int num_chunks = 0;

	ObjectPoolChunk* chunk = pool;
	while (chunk != NULL)
	{
		++num_chunks;
		chunk = chunk->next;
	}

	return num_chunks;
}

// Returns the number of allocated objects in the pool.
template < typename PoolType >
int ObjectPool< PoolType >::GetNumAllocatedObjects() const
{
	return num_allocated_objects;
}

// Creates a new pool chunk and appends its nodes to the beginning of the free list.
template < typename PoolType >
void ObjectPool< PoolType >::CreateChunk()
{
	if (chunk_size <= 0)
		return;

	// Create the new chunk and mark it as the first chunk.
	ObjectPoolChunk* new_chunk = new ObjectPoolChunk();
	new_chunk->next = pool;
	pool = new_chunk;

	// Create chunk's pool nodes.
	new_chunk->chunk = new ObjectPoolNode[chunk_size];

	// Initialise the linked list.
	for (int i = 0; i < chunk_size; i++)
	{
		if (i == 0)
			new_chunk->chunk[i].previous = NULL ;
		else
			new_chunk->chunk[i].previous = &new_chunk->chunk[i - 1];

		if (i == chunk_size - 1)
			new_chunk->chunk[i].next = first_free_node;
		else
			new_chunk->chunk[i].next = &new_chunk->chunk[i + 1];
	}

	first_free_node = new_chunk->chunk;
}

}


