package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal {
      math.pow(b(), 2) - 4*a()*c()
    }
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal {
      val deltaVal = delta()
      if(deltaVal < 0) Set()
      else if(deltaVal == 0) {
        Set(-b() / 2*a())
      } else {
        Set(-b() - math.sqrt(deltaVal) / 2*a(),
          -b() + math.sqrt(deltaVal) / 2*a())
      }
    }
  }
}
