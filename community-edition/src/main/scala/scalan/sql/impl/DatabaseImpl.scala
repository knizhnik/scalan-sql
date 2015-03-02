package scalan.sql
package impl

import scalan._
import scalan.common.Default
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait DatabaseAbs extends Scalan with Database {
  self: DatabaseDsl =>
  // single proxy for each type family
  implicit def proxySession(p: Rep[Session]): Session = {
    implicit val tag = weakTypeTag[Session]
    proxyOps[Session](p)(TagImplicits.typeTagToClassTag[Session])
  }

  abstract class SessionElem[From, To <: Session](iso: Iso[From, To])
    extends ViewElem[From, To](iso) {
    override def convert(x: Rep[Reifiable[_]]) = convertSession(x.asRep[Session])
    def convertSession(x : Rep[Session]): Rep[To]
  }

  trait SessionCompanionElem extends CompanionElem[SessionCompanionAbs]
  implicit lazy val SessionCompanionElem: SessionCompanionElem = new SessionCompanionElem {
    lazy val tag = weakTypeTag[SessionCompanionAbs]
    protected def getDefaultRep = Session
  }

  abstract class SessionCompanionAbs extends CompanionBase[SessionCompanionAbs] with SessionCompanion {
    override def toString = "Session"
  }
  def Session: Rep[SessionCompanionAbs]
  implicit def proxySessionCompanion(p: Rep[SessionCompanion]): SessionCompanion = {
    proxyOps[SessionCompanion](p)
  }

  // elem for concrete class
  class DatabaseSessionElem(iso: Iso[DatabaseSessionData, DatabaseSession])
    extends SessionElem[DatabaseSessionData, DatabaseSession](iso) {
    def convertSession(x: Rep[Session]) = // Converter is not generated by meta
!!!("Cannot convert from Session to DatabaseSession: missing fields List(login)")
  }

  // state representation type
  type DatabaseSessionData = String

  // 3) Iso for concrete class
  class DatabaseSessionIso
    extends Iso[DatabaseSessionData, DatabaseSession] {
    override def from(p: Rep[DatabaseSession]) =
      unmkDatabaseSession(p) match {
        case Some((login)) => login
        case None => !!!
      }
    override def to(p: Rep[String]) = {
      val login = p
      DatabaseSession(login)
    }
    lazy val tag = {
      weakTypeTag[DatabaseSession]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[DatabaseSession]](DatabaseSession(""))
    lazy val eTo = new DatabaseSessionElem(this)
  }
  // 4) constructor and deconstructor
  abstract class DatabaseSessionCompanionAbs extends CompanionBase[DatabaseSessionCompanionAbs] with DatabaseSessionCompanion {
    override def toString = "DatabaseSession"

    def apply(login: Rep[String]): Rep[DatabaseSession] =
      mkDatabaseSession(login)
    def unapply(p: Rep[DatabaseSession]) = unmkDatabaseSession(p)
  }
  def DatabaseSession: Rep[DatabaseSessionCompanionAbs]
  implicit def proxyDatabaseSessionCompanion(p: Rep[DatabaseSessionCompanionAbs]): DatabaseSessionCompanionAbs = {
    proxyOps[DatabaseSessionCompanionAbs](p)
  }

  class DatabaseSessionCompanionElem extends CompanionElem[DatabaseSessionCompanionAbs] {
    lazy val tag = weakTypeTag[DatabaseSessionCompanionAbs]
    protected def getDefaultRep = DatabaseSession
  }
  implicit lazy val DatabaseSessionCompanionElem: DatabaseSessionCompanionElem = new DatabaseSessionCompanionElem

  implicit def proxyDatabaseSession(p: Rep[DatabaseSession]): DatabaseSession =
    proxyOps[DatabaseSession](p)

  implicit class ExtendedDatabaseSession(p: Rep[DatabaseSession]) {
    def toData: Rep[DatabaseSessionData] = isoDatabaseSession.from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoDatabaseSession: Iso[DatabaseSessionData, DatabaseSession] =
    new DatabaseSessionIso

  // 6) smart constructor and deconstructor
  def mkDatabaseSession(login: Rep[String]): Rep[DatabaseSession]
  def unmkDatabaseSession(p: Rep[DatabaseSession]): Option[(Rep[String])]
}

// Seq -----------------------------------
trait DatabaseSeq extends DatabaseDsl with ScalanSeq {
  self: DatabaseDslSeq =>
  lazy val Session: Rep[SessionCompanionAbs] = new SessionCompanionAbs with UserTypeSeq[SessionCompanionAbs, SessionCompanionAbs] {
    lazy val selfType = element[SessionCompanionAbs]
  }

  case class SeqDatabaseSession
      (override val login: Rep[String])

    extends DatabaseSession(login)
        with UserTypeSeq[Session, DatabaseSession] {
    lazy val selfType = element[DatabaseSession].asInstanceOf[Elem[Session]]
  }
  lazy val DatabaseSession = new DatabaseSessionCompanionAbs with UserTypeSeq[DatabaseSessionCompanionAbs, DatabaseSessionCompanionAbs] {
    lazy val selfType = element[DatabaseSessionCompanionAbs]
  }

  def mkDatabaseSession
      (login: Rep[String]): Rep[DatabaseSession] =
      new SeqDatabaseSession(login)
  def unmkDatabaseSession(p: Rep[DatabaseSession]) =
    Some((p.login))
}

// Exp -----------------------------------
trait DatabaseExp extends DatabaseDsl with ScalanExp {
  self: DatabaseDslExp =>
  lazy val Session: Rep[SessionCompanionAbs] = new SessionCompanionAbs with UserTypeDef[SessionCompanionAbs, SessionCompanionAbs] {
    lazy val selfType = element[SessionCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  case class ExpDatabaseSession
      (override val login: Rep[String])

    extends DatabaseSession(login) with UserTypeDef[Session, DatabaseSession] {
    lazy val selfType = element[DatabaseSession].asInstanceOf[Elem[Session]]
    override def mirror(t: Transformer) = ExpDatabaseSession(t(login))
  }

  lazy val DatabaseSession: Rep[DatabaseSessionCompanionAbs] = new DatabaseSessionCompanionAbs with UserTypeDef[DatabaseSessionCompanionAbs, DatabaseSessionCompanionAbs] {
    lazy val selfType = element[DatabaseSessionCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object DatabaseSessionMethods {
  }

  object DatabaseSessionCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[DatabaseSessionCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkDatabaseSession
    (login: Rep[String]): Rep[DatabaseSession] =
    new ExpDatabaseSession(login)
  def unmkDatabaseSession(p: Rep[DatabaseSession]) =
    Some((p.login))

  object SessionMethods {
  }

  object SessionCompanionMethods {
  }
}
