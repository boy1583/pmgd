#pragma once
#include "property.h"
#include "iterator.h"

namespace Jarvis {
    class Node;
    class Allocator;

    // Base class for all the property value indices
    class Index {
        PropertyType _ptype;
    public:
        void init(PropertyType ptype);
        void add(const Property &p, Node *n, Allocator &allocator);
        void remove(const Property &p, Node *n, Allocator &allocator);

        NodeIterator get_nodes(const PropertyPredicate &pp);
    };
}
