//
//  timer.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_TIMER_H_
#define YCSB_C_TIMER_H_

#include <chrono>

namespace utils {
class Timer {
 public:
    typedef std::chrono::seconds sec;
    typedef std::chrono::milliseconds milli;
    typedef std::chrono::microseconds micro;
    typedef std::chrono::nanoseconds nano;
  void Start() {
    time_ = Clock::now();
  }
  template <typename T>
  double End() {
    return std::chrono::duration_cast<T>(Clock::now() - time_).count();
  }

 private:
  typedef std::chrono::high_resolution_clock Clock;
  Clock::time_point time_;
};

} // utils

#endif // YCSB_C_TIMER_H_

