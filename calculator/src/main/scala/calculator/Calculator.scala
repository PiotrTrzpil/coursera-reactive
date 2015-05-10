package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(
      namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {
      namedExpressions.map { case (key,value) => key -> Signal(evaluate(key, value(), namedExpressions))}
  }

  def evaluate(rootName:String, expr: Expr, references: Map[String, Signal[Expr]]):Double = {
    def eval(expr: Expr): Double = {
      expr match {
        case Literal(value) => value
        case Ref(name) =>
          val refer = getReferenceExpr(name, references)
          eval(if(name == rootName) Literal(Double.NaN) else refer)
        case Plus(a, b) => eval(a) + eval(b)
        case Minus(a, b) => eval(a) - eval(b)
        case Times(a, b) => eval(a) * eval(b)
        case Divide(a, b) => eval(a) / eval(b)
      }
    }
    eval(expr)
  }


  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
