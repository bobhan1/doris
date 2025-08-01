// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <cassert>
#include <cstddef>
#include <utility> // IWYU pragma: keep

#include "butil/macros.h"
#include "gutil/atomicops.h"

namespace doris {
namespace subtle {

typedef Atomic32 AtomicRefCount;

class RefCountedThreadSafeBase {
public:
    bool HasOneRef() const;

protected:
    RefCountedThreadSafeBase();
    ~RefCountedThreadSafeBase();

    void AddRef() const;

    // Returns true if the object should self-delete.
    bool Release() const;

private:
    mutable AtomicRefCount ref_count_;
#ifndef NDEBUG
    mutable bool in_dtor_;
#endif

    DISALLOW_COPY_AND_ASSIGN(RefCountedThreadSafeBase);
};

} // namespace subtle

// Forward declaration.
template <class T, typename Traits>
class RefCountedThreadSafe;

// Default traits for RefCountedThreadSafe<T>.  Deletes the object when its ref
// count reaches 0.  Overload to delete it on a different thread etc.
template <typename T>
struct DefaultRefCountedThreadSafeTraits {
    static void Destruct(const T* x) {
        // Delete through RefCountedThreadSafe to make child classes only need to be
        // friend with RefCountedThreadSafe instead of this struct, which is an
        // implementation detail.
        RefCountedThreadSafe<T, DefaultRefCountedThreadSafeTraits>::DeleteInternal(x);
    }
};

//
// A thread-safe variant of RefCounted<T>
//
//   class MyFoo : public RefCountedThreadSafe<MyFoo> {
//    ...
//   };
//
// If you're using the default trait, then you should add compile time
// asserts that no one else is deleting your object.  i.e.
//    private:
//     friend class RefCountedThreadSafe<MyFoo>;
//     ~MyFoo();
template <class T, typename Traits = DefaultRefCountedThreadSafeTraits<T>>
class RefCountedThreadSafe : public subtle::RefCountedThreadSafeBase {
public:
    RefCountedThreadSafe() {}

    void AddRef() const { subtle::RefCountedThreadSafeBase::AddRef(); }

    void Release() const {
        if (subtle::RefCountedThreadSafeBase::Release()) {
            Traits::Destruct(static_cast<const T*>(this));
        }
    }

protected:
    ~RefCountedThreadSafe() {}

private:
    friend struct DefaultRefCountedThreadSafeTraits<T>;
    static void DeleteInternal(const T* x) { delete x; }

    DISALLOW_COPY_AND_ASSIGN(RefCountedThreadSafe);
};

} // namespace doris

//
// A smart pointer class for reference counted objects.  Use this class instead
// of calling AddRef and Release manually on a reference counted object to
// avoid common memory leaks caused by forgetting to Release an object
// reference.  Sample usage:
//
//   class MyFoo : public RefCounted<MyFoo> {
//    ...
//   };
//
//   void some_function() {
//     scoped_refptr<MyFoo> foo = new MyFoo();
//     foo->Method(param);
//     // |foo| is released when this function returns
//   }
//
//   void some_other_function() {
//     scoped_refptr<MyFoo> foo = new MyFoo();
//     ...
//     foo = NULL;  // explicitly releases |foo|
//     ...
//     if (foo)
//       foo->Method(param);
//   }
//
// The above examples show how scoped_refptr<T> acts like a pointer to T.
// Given two scoped_refptr<T> classes, it is also possible to exchange
// references between the two objects, like so:
//
//   {
//     scoped_refptr<MyFoo> a = new MyFoo();
//     scoped_refptr<MyFoo> b;
//
//     b.swap(a);
//     // now, |b| references the MyFoo object, and |a| references NULL.
//   }
//
// To make both |a| and |b| in the above example reference the same MyFoo
// object, simply use the assignment operator:
//
//   {
//     scoped_refptr<MyFoo> a = new MyFoo();
//     scoped_refptr<MyFoo> b;
//
//     b = a;
//     // now, |a| and |b| each own a reference to the same MyFoo object.
//   }
//
template <class T>
class scoped_refptr {
public:
    typedef T element_type;

    scoped_refptr() : ptr_(NULL) {}

    scoped_refptr(T* p) : ptr_(p) {
        if (ptr_) ptr_->AddRef();
    }

    // Copy constructor.
    scoped_refptr(const scoped_refptr<T>& r) : ptr_(r.ptr_) {
        if (ptr_) ptr_->AddRef();
    }

    // Copy conversion constructor.
    template <typename U>
    scoped_refptr(const scoped_refptr<U>& r) : ptr_(r.get()) {
        if (ptr_) ptr_->AddRef();
    }

    // Move constructor. This is required in addition to the conversion
    // constructor below in order for clang to warn about pessimizing moves.
    scoped_refptr(scoped_refptr&& r) noexcept : ptr_(r.get()) { // NOLINT
        r.ptr_ = nullptr;
    }

    // Move conversion constructor.
    template <typename U>
    scoped_refptr(scoped_refptr<U>&& r) noexcept : ptr_(r.get()) { // NOLINT
        r.ptr_ = nullptr;
    }

    ~scoped_refptr() {
        if (ptr_) ptr_->Release();
    }

    T* get() const { return ptr_; }

    typedef T* scoped_refptr::* Testable;
    operator Testable() const { return ptr_ ? &scoped_refptr::ptr_ : NULL; }

    T* operator->() const {
        assert(ptr_ != NULL);
        return ptr_;
    }

    scoped_refptr<T>& operator=(T* p) {
        // AddRef first so that self assignment should work
        if (p) p->AddRef();
        T* old_ptr = ptr_;
        ptr_ = p;
        if (old_ptr) old_ptr->Release();
        return *this;
    }

    scoped_refptr<T>& operator=(const scoped_refptr<T>& r) { return *this = r.ptr_; }

    template <typename U>
    scoped_refptr<T>& operator=(const scoped_refptr<U>& r) {
        return *this = r.get();
    }

    scoped_refptr<T>& operator=(scoped_refptr<T>&& r) {
        scoped_refptr<T>(std::move(r)).swap(*this);
        return *this;
    }

    template <typename U>
    scoped_refptr<T>& operator=(scoped_refptr<U>&& r) {
        scoped_refptr<T>(std::move(r)).swap(*this);
        return *this;
    }

    void swap(T** pp) {
        T* p = ptr_;
        ptr_ = *pp;
        *pp = p;
    }

    void swap(scoped_refptr<T>& r) { swap(&r.ptr_); }

    // Like gscoped_ptr::reset(), drops a reference on the currently held object
    // (if any), and adds a reference to the passed-in object (if not NULL).
    void reset(T* p = NULL) { *this = p; }

protected:
    T* ptr_ = nullptr;

private:
    template <typename U>
    friend class scoped_refptr;
};
