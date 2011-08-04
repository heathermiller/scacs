package scacs

import java.util.concurrent.locks.ReentrantLock

// this multi-buffer is static. if necessary, we can include the dynamic one one day...
final class MultiBuffer[@specialized T](size: Int) {

  private val buffers = Array.fill(size)(new Buffer[T])

  def get(index: Int): T = {
    buffers(index).get
  }

  /* could allow a thread to grab its buffer and then call get directly in the future
  def getBuffer(index: Int): Buffer[T] = {
    buffers(index)
  } */

  def put(value: T) {
    var i = 0
    while (i < size) {
      buffers(i).put(value)
      i += 1
    }
  }
  
}

final class Buffer[@specialized T] {

  private var isEmpty: Boolean = true
  private var _value: T = _

  private val lock = new ReentrantLock
  private val notEmpty = lock.newCondition()
  private val notFull = lock.newCondition()

  def get: T = {
    val lock = this.lock
    lock.lock()
    try {
      while(isEmpty) {
        notEmpty.await()
      }
      val value = _value
      _value = null.asInstanceOf[T]
      isEmpty = true
      notFull.signal()
      value
    }
    finally {
      lock.unlock()
    }
  }

  def put(value: T) {
    val lock = this.lock
    lock.lock()
    try {
      while (!isEmpty) {
        notFull.await()
      }
      _value = value
      isEmpty = false
      notEmpty.signal()
    }
    finally {
      lock.unlock()
    }
  }

}