/*
 * TODOs
 * - Write random data in regions in debug mode
 * - signature checking?
 * - throw an exception instead of returning NULL
 */

#include <stddef.h>
#include <assert.h>

#include "exception.h"
#include "FixedAllocator.h"
#include "TransactionImpl.h"

using namespace Jarvis;

#define ALLOC_OFFSET(sz) ((sizeof(RegionHeader) + (sz) - 1) & ~((sz) - 1))
FixedAllocator::FixedAllocator(uint64_t pool_addr, RegionHeader *hdr_addr,
                               uint32_t object_size, uint64_t pool_size,
                               bool create)
    : _pm(hdr_addr),
      _pool_addr(pool_addr)
{
    if ((uint64_t)hdr_addr == pool_addr)
        _alloc_offset = ALLOC_OFFSET(create ? object_size : _pm->size);
    else
        _alloc_offset = 0;

    if (create) {
        // Object size must be a power of 2 and at least 8 bytes.
        assert(object_size >= sizeof(uint64_t));
        assert(!(object_size & (object_size - 1)));
        // Make sure we have a well-aligned pool_addr.
        assert((_pool_addr & (object_size - 1)) == 0);

        _pm->tail_ptr = (uint64_t *)(_pool_addr + _alloc_offset);
        _pm->free_ptr = NULL;
        _pm->num_allocated = 0;
        _pm->max_addr = pool_addr + pool_size;
        _pm->size = object_size;

        TransactionImpl::flush_range(_pm, sizeof(*_pm));
    }
}

FixedAllocator::FixedAllocator(uint64_t pool_addr,
                               uint32_t object_size, uint64_t pool_size,
                               bool create)
    : FixedAllocator(pool_addr, reinterpret_cast<RegionHeader *>(pool_addr),
                     object_size, pool_size,
                     create)
{ }

void *FixedAllocator::alloc()
{
    TransactionImpl *tx = TransactionImpl::get_tx();

    tx->log_range(&_pm->tail_ptr, &_pm->num_allocated);

    uint64_t *p;
    if (_pm->free_ptr != NULL) {
        /* We found an object on the free list */
        p = _pm->free_ptr;

        // Log the 8-byte freelist pointer in the allocated object.
        tx->log(p, sizeof(uint64_t));

        *p &= ~FREE_BIT;
        _pm->free_ptr = (uint64_t *)*_pm->free_ptr;
    }
    else
    {
        if (((uint64_t)_pm->tail_ptr + _pm->size) > _pm->max_addr)
            throw Exception(BadAlloc);

        /* Free list exhausted, we are growing our pool of objects by one */
        p = _pm->tail_ptr;
        _pm->tail_ptr = (uint64_t *)((uint64_t)_pm->tail_ptr + _pm->size);
    }

    _pm->num_allocated++;

    return p;
}

void *FixedAllocator::alloc(unsigned num)
{
    // Makes sense to optimize here to ensure the
    // free list can get used.
    if (num == 1)
        return alloc();

    TransactionImpl *tx = TransactionImpl::get_tx();

    uint64_t *p;

    // Skip checking the free list since those pages may or may
    // not be contiguous. However, this could lead to the pathological
    // case where say some application allocated multiple such spots
    // but freed them later and those were then put in the free list
    // as usual. It could cause allocator to refuse contiguous requests
    // even if there was enough space. But keeping it simple for now.
    if (((uint64_t)_pm->tail_ptr + num * _pm->size) > _pm->max_addr)
        throw Exception(BadAlloc);

    p = _pm->tail_ptr;
    tx->write(&_pm->tail_ptr, (uint64_t *)((uint64_t)_pm->tail_ptr + num * _pm->size));

    tx->write(&_pm->num_allocated, _pm->num_allocated + num);

    return p;
}

/**
 * Free an object
 *
 * A simple implementation that inserts the freed object at the
 * beginning of the free list.
 */
void FixedAllocator::free(void *p)
{
    // Check to make sure given address was allocated from this allocator.
    assert(p >= (void *)((uint64_t)_pool_addr + _alloc_offset) && p < _pm->tail_ptr);
    // Check to make sure it is a multiple of the size.
    assert((uint64_t)p % _pm->size == 0);
    // Check to make sure this object was indeed allocated.
    // assert(Check free list for p);
    TransactionImpl *tx = TransactionImpl::get_tx();

    tx->log(p, sizeof(uint64_t));
    *(uint64_t *)p = FREE_BIT;

    AllocatorCallback<FixedAllocator, void *>::delayed_free(tx, this, p);
}

void FixedAllocator::clean_free_list
    (TransactionImpl *tx, const std::list<void *> &list)
{
    tx->log_range(&_pm->free_ptr, &_pm->num_allocated);
    int64_t num_allocated = _pm->num_allocated;
    void *free_ptr = _pm->free_ptr;

    for (auto p : list) {
        *(uint64_t *)p = (uint64_t)free_ptr | FREE_BIT;
        free_ptr = p;
        num_allocated--;
    }

    _pm->free_ptr = (uint64_t *)free_ptr;
    _pm->num_allocated = num_allocated;
}

void FixedAllocator::free(void *p, unsigned num)
{
    /* *** Consider the following here after figuring out delayed
     * free for this function:
     * if (((uint64_t)p + _pm->size * num) == (uint64_t)_pm->tail_ptr)
     *     _pm->tail_ptr = (uint64_t *)p;
     * else
     */
    for (unsigned i = 0; i < num; ++i) {
        free(p);
        p = (void *)((uint64_t)p + _pm->size);
    }
}
