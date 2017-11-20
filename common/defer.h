// Copyright: ThoughtSpot Inc. 2017
// Author: Priyendra Deshwal (deshwal@thoughtspot.com)

#ifndef COMMON_DEFER_HPP_
#define COMMON_DEFER_HPP_

#include <functional>
#include <vector>
#include <utility>

namespace thoughtspot {
namespace common {

// A ScopedLambda is a simple wrapper around a lambda which ensures execution
// of the lambda function upon scope-exit. This is useful in situations where
// we need custom cleanup which is not possible using std::unique_ptr.
// For example, consider the case where we have a vector of pointers which
// need to be deleted.
// {
//   std::vector<MyClass*> objects;
//   auto objects_deleter = MakeScopedLambda([&]() {
//       STLDeleteElements(&objects);
//   });
// }  // elements in @objects will be deleted.
template <typename FnType>
class ScopedLambda {
 public:
  explicit ScopedLambda(FnType fn) : fn_(std::move(fn)), active_(true) { }
  // Default movable.
  ScopedLambda(ScopedLambda&&) = default;
  ScopedLambda& operator=(ScopedLambda&&) = default;
  // Non-copyable. In particular, there is no good reasoning about which copy
  // remains active.
  ScopedLambda(const ScopedLambda&) = delete;
  ScopedLambda& operator=(const ScopedLambda&) = delete;
  ~ScopedLambda() {
    if (active_) fn_();
  }
  void run_and_expire() {
    if (active_) fn_();
    active_ = false;
  }
  void activate() { active_ = true; }
  void deactivate() { active_ = false; }

 private:
  FnType fn_;
  bool active_ = true;
};

template <typename FnType>
ScopedLambda<FnType> MakeScopedLambda(FnType fn) {
  return ScopedLambda<FnType>(std::move(fn));
}

}  // namespace common
}  // namespace thoughtspot

// ScopedLambda can often be quite verbose. DEFER is modeled on the defer
// keyword introduced by Go which is a way of deferring the execution of
// code to scope exit. NAMED_DEFER allows users to name the internal scoped
// lambda created as part of the defer process. This is useful in case users
// wish to deactivate the lambda at a later point in time.
//
// Examples
// 1.  std::vector<MyObject*> objects;
//     DEFER(STLDeleteElements(&objects));
// 2.  std::vector<MyObject*> objects;
//     DEFER(for (const MyObject* obj : objects) delete obj; );
// 3.  std::vector<MyObject*> objects;
//     NAMED_DEFER(objects_deleter, STLDeleteElements(&objects));
//     objects_deleter.deactivate();
//
// See unittest for some more examples.

#define TOKEN_PASTE(x, y) x ## y
#define TOKEN_PASTE2(x, y) TOKEN_PASTE(x, y)
#define SCOPE_UNIQUE_NAME(name) TOKEN_PASTE2(name, __LINE__)
#define DEFER(...)                                                  \
    auto SCOPE_UNIQUE_NAME(defer_varname) =                         \
        common::MakeScopedLambda([&] {                            \
            __VA_ARGS__;                                            \
        });
#define NAMED_DEFER(name, ...)                                      \
    auto name = common::MakeScopedLambda([&] { __VA_ARGS__; })
#define DEFER_SET(name)                                             \
    common::ScopedLambdaSet name;

#endif  // COMMON_DEFER_HPP_
