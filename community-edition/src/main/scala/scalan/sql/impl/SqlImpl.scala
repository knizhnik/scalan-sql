package scalan.sql
package impl

import scalan._
import scalan.collections._
import scalan.common.Default
import scala.reflect.runtime.universe._
import scalan.common.Default

// Abs -----------------------------------
trait SqlAbs extends Scalan with Sql {
  self: SqlDsl =>
  // single proxy for each type family
  implicit def proxyTable[R](p: Rep[Table[R]]): Table[R] =
    proxyOps[Table[R]](p)

  abstract class TableElem[R, From, To <: Table[R]](iso: Iso[From, To]) extends ViewElem[From, To]()(iso)

  trait TableCompanionElem extends CompanionElem[TableCompanionAbs]
  implicit lazy val TableCompanionElem: TableCompanionElem = new TableCompanionElem {
    lazy val tag = weakTypeTag[TableCompanionAbs]
    protected def getDefaultRep = Table
  }

  abstract class TableCompanionAbs extends CompanionBase[TableCompanionAbs] with TableCompanion {
    override def toString = "Table"
  }
  def Table: Rep[TableCompanionAbs]
  implicit def proxyTableCompanion(p: Rep[TableCompanion]): TableCompanion = {
    proxyOps[TableCompanion](p)
  }

  // elem for concrete class
  class BaseTableElem[R](iso: Iso[BaseTableData[R], BaseTable[R]]) extends TableElem[R, BaseTableData[R], BaseTable[R]](iso)

  // state representation type
  type BaseTableData[R] = String

  // 3) Iso for concrete class
  class BaseTableIso[R](implicit schema: Elem[R])
    extends Iso[BaseTableData[R], BaseTable[R]] {
    override def from(p: Rep[BaseTable[R]]) =
      unmkBaseTable(p) match {
        case Some((tableName)) => tableName
        case None => !!!
      }
    override def to(p: Rep[String]) = {
      val tableName = p
      BaseTable(tableName)
    }
    lazy val tag = {
      weakTypeTag[BaseTable[R]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[BaseTable[R]]](BaseTable(""))
    lazy val eTo = new BaseTableElem[R](this)
  }
  // 4) constructor and deconstructor
  abstract class BaseTableCompanionAbs extends CompanionBase[BaseTableCompanionAbs] with BaseTableCompanion {
    override def toString = "BaseTable"

    def apply[R](tableName: Rep[String])(implicit schema: Elem[R]): Rep[BaseTable[R]] =
      mkBaseTable(tableName)
    def unapply[R:Elem](p: Rep[BaseTable[R]]) = unmkBaseTable(p)
  }
  def BaseTable: Rep[BaseTableCompanionAbs]
  implicit def proxyBaseTableCompanion(p: Rep[BaseTableCompanionAbs]): BaseTableCompanionAbs = {
    proxyOps[BaseTableCompanionAbs](p)
  }

  class BaseTableCompanionElem extends CompanionElem[BaseTableCompanionAbs] {
    lazy val tag = weakTypeTag[BaseTableCompanionAbs]
    protected def getDefaultRep = BaseTable
  }
  implicit lazy val BaseTableCompanionElem: BaseTableCompanionElem = new BaseTableCompanionElem

  implicit def proxyBaseTable[R](p: Rep[BaseTable[R]]): BaseTable[R] =
    proxyOps[BaseTable[R]](p)

  implicit class ExtendedBaseTable[R](p: Rep[BaseTable[R]])(implicit schema: Elem[R]) {
    def toData: Rep[BaseTableData[R]] = isoBaseTable(schema).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoBaseTable[R](implicit schema: Elem[R]): Iso[BaseTableData[R], BaseTable[R]] =
    new BaseTableIso[R]

  // 6) smart constructor and deconstructor
  def mkBaseTable[R](tableName: Rep[String])(implicit schema: Elem[R]): Rep[BaseTable[R]]
  def unmkBaseTable[R:Elem](p: Rep[BaseTable[R]]): Option[(Rep[String])]

  // elem for concrete class
  class UniqueIndexElem[K, R](iso: Iso[UniqueIndexData[K, R], UniqueIndex[K, R]]) extends TableElem[R, UniqueIndexData[K, R], UniqueIndex[K, R]](iso)

  // state representation type
  type UniqueIndexData[K, R] = (String, (Table[R], (MMap[K,R], R => K)))

  // 3) Iso for concrete class
  class UniqueIndexIso[K, R](implicit schema: Elem[R], index: Elem[K], keyPath: String)
    extends Iso[UniqueIndexData[K, R], UniqueIndex[K, R]] {
    override def from(p: Rep[UniqueIndex[K, R]]) =
      unmkUniqueIndex(p) match {
        case Some((tableName, table, map, getKey)) => Pair(tableName, Pair(table, Pair(map, getKey)))
        case None => !!!
      }
    override def to(p: Rep[(String, (Table[R], (MMap[K,R], R => K)))]) = {
      val Pair(tableName, Pair(table, Pair(map, getKey))) = p
      UniqueIndex(tableName, table, map, getKey)
    }
    lazy val tag = {
      weakTypeTag[UniqueIndex[K, R]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[UniqueIndex[K, R]]](UniqueIndex("", element[Table[R]].defaultRepValue, element[MMap[K,R]].defaultRepValue, fun { (x: Rep[R]) => element[K].defaultRepValue }))
    lazy val eTo = new UniqueIndexElem[K, R](this)
  }
  // 4) constructor and deconstructor
  abstract class UniqueIndexCompanionAbs extends CompanionBase[UniqueIndexCompanionAbs] with UniqueIndexCompanion {
    override def toString = "UniqueIndex"
    def apply[K, R](p: Rep[UniqueIndexData[K, R]])(implicit schema: Elem[R], index: Elem[K], keyPath: String): Rep[UniqueIndex[K, R]] =
      isoUniqueIndex(schema, index, keyPath).to(p)
    def apply[K, R](tableName: Rep[String], table: Rep[Table[R]], map: Rep[MMap[K,R]], getKey: Rep[R => K])(implicit schema: Elem[R], index: Elem[K], keyPath: String): Rep[UniqueIndex[K, R]] =
      mkUniqueIndex(tableName, table, map, getKey)
    def unapply[K:Elem, R:Elem](p: Rep[UniqueIndex[K, R]]) = unmkUniqueIndex(p)
  }
  def UniqueIndex: Rep[UniqueIndexCompanionAbs]
  implicit def proxyUniqueIndexCompanion(p: Rep[UniqueIndexCompanionAbs]): UniqueIndexCompanionAbs = {
    proxyOps[UniqueIndexCompanionAbs](p)
  }

  class UniqueIndexCompanionElem extends CompanionElem[UniqueIndexCompanionAbs] {
    lazy val tag = weakTypeTag[UniqueIndexCompanionAbs]
    protected def getDefaultRep = UniqueIndex
  }
  implicit lazy val UniqueIndexCompanionElem: UniqueIndexCompanionElem = new UniqueIndexCompanionElem

  implicit def proxyUniqueIndex[K, R](p: Rep[UniqueIndex[K, R]]): UniqueIndex[K, R] =
    proxyOps[UniqueIndex[K, R]](p)

  implicit class ExtendedUniqueIndex[K, R](p: Rep[UniqueIndex[K, R]])(implicit schema: Elem[R], index: Elem[K], keyPath: String) {
    def toData: Rep[UniqueIndexData[K, R]] = isoUniqueIndex(schema, index, keyPath).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoUniqueIndex[K, R](implicit schema: Elem[R], index: Elem[K], keyPath: String): Iso[UniqueIndexData[K, R], UniqueIndex[K, R]] =
    new UniqueIndexIso[K, R]

  // 6) smart constructor and deconstructor
  def mkUniqueIndex[K, R](tableName: Rep[String], table: Rep[Table[R]], map: Rep[MMap[K,R]], getKey: Rep[R => K])(implicit schema: Elem[R], index: Elem[K], keyPath: String): Rep[UniqueIndex[K, R]]
  def unmkUniqueIndex[K:Elem, R:Elem](p: Rep[UniqueIndex[K, R]]): Option[(Rep[String], Rep[Table[R]], Rep[MMap[K,R]], Rep[R => K])]

  // elem for concrete class
  class NonUniqueIndexElem[K, R](iso: Iso[NonUniqueIndexData[K, R], NonUniqueIndex[K, R]]) extends TableElem[R, NonUniqueIndexData[K, R], NonUniqueIndex[K, R]](iso)

  // state representation type
  type NonUniqueIndexData[K, R] = (String, (Table[R], (MMultiMap[K,R], R => K)))

  // 3) Iso for concrete class
  class NonUniqueIndexIso[K, R](implicit schema: Elem[R], index: Elem[K], keyPath: String)
    extends Iso[NonUniqueIndexData[K, R], NonUniqueIndex[K, R]] {
    override def from(p: Rep[NonUniqueIndex[K, R]]) =
      unmkNonUniqueIndex(p) match {
        case Some((tableName, table, map, getKey)) => Pair(tableName, Pair(table, Pair(map, getKey)))
        case None => !!!
      }
    override def to(p: Rep[(String, (Table[R], (MMultiMap[K,R], R => K)))]) = {
      val Pair(tableName, Pair(table, Pair(map, getKey))) = p
      NonUniqueIndex(tableName, table, map, getKey)
    }
    lazy val tag = {
      weakTypeTag[NonUniqueIndex[K, R]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[NonUniqueIndex[K, R]]](NonUniqueIndex("", element[Table[R]].defaultRepValue, element[MMultiMap[K,R]].defaultRepValue, fun { (x: Rep[R]) => element[K].defaultRepValue }))
    lazy val eTo = new NonUniqueIndexElem[K, R](this)
  }
  // 4) constructor and deconstructor
  abstract class NonUniqueIndexCompanionAbs extends CompanionBase[NonUniqueIndexCompanionAbs] with NonUniqueIndexCompanion {
    override def toString = "NonUniqueIndex"
    def apply[K, R](p: Rep[NonUniqueIndexData[K, R]])(implicit schema: Elem[R], index: Elem[K], keyPath: String): Rep[NonUniqueIndex[K, R]] =
      isoNonUniqueIndex(schema, index, keyPath).to(p)
    def apply[K, R](tableName: Rep[String], table: Rep[Table[R]], map: Rep[MMultiMap[K,R]], getKey: Rep[R => K])(implicit schema: Elem[R], index: Elem[K], keyPath: String): Rep[NonUniqueIndex[K, R]] =
      mkNonUniqueIndex(tableName, table, map, getKey)
    def unapply[K:Elem, R:Elem](p: Rep[NonUniqueIndex[K, R]]) = unmkNonUniqueIndex(p)
  }
  def NonUniqueIndex: Rep[NonUniqueIndexCompanionAbs]
  implicit def proxyNonUniqueIndexCompanion(p: Rep[NonUniqueIndexCompanionAbs]): NonUniqueIndexCompanionAbs = {
    proxyOps[NonUniqueIndexCompanionAbs](p)
  }

  class NonUniqueIndexCompanionElem extends CompanionElem[NonUniqueIndexCompanionAbs] {
    lazy val tag = weakTypeTag[NonUniqueIndexCompanionAbs]
    protected def getDefaultRep = NonUniqueIndex
  }
  implicit lazy val NonUniqueIndexCompanionElem: NonUniqueIndexCompanionElem = new NonUniqueIndexCompanionElem

  implicit def proxyNonUniqueIndex[K, R](p: Rep[NonUniqueIndex[K, R]]): NonUniqueIndex[K, R] =
    proxyOps[NonUniqueIndex[K, R]](p)

  implicit class ExtendedNonUniqueIndex[K, R](p: Rep[NonUniqueIndex[K, R]])(implicit schema: Elem[R], index: Elem[K], keyPath: String) {
    def toData: Rep[NonUniqueIndexData[K, R]] = isoNonUniqueIndex(schema, index, keyPath).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoNonUniqueIndex[K, R](implicit schema: Elem[R], index: Elem[K], keyPath: String): Iso[NonUniqueIndexData[K, R], NonUniqueIndex[K, R]] =
    new NonUniqueIndexIso[K, R]

  // 6) smart constructor and deconstructor
  def mkNonUniqueIndex[K, R](tableName: Rep[String], table: Rep[Table[R]], map: Rep[MMultiMap[K,R]], getKey: Rep[R => K])(implicit schema: Elem[R], index: Elem[K], keyPath: String): Rep[NonUniqueIndex[K, R]]
  def unmkNonUniqueIndex[K:Elem, R:Elem](p: Rep[NonUniqueIndex[K, R]]): Option[(Rep[String], Rep[Table[R]], Rep[MMultiMap[K,R]], Rep[R => K])]

  // elem for concrete class
  class ReadWriteTableElem[R](iso: Iso[ReadWriteTableData[R], ReadWriteTable[R]]) extends TableElem[R, ReadWriteTableData[R], ReadWriteTable[R]](iso)

  // state representation type
  type ReadWriteTableData[R] = (String, ArrayBuffer[R])

  // 3) Iso for concrete class
  class ReadWriteTableIso[R](implicit schema: Elem[R])
    extends Iso[ReadWriteTableData[R], ReadWriteTable[R]] {
    override def from(p: Rep[ReadWriteTable[R]]) =
      unmkReadWriteTable(p) match {
        case Some((tableName, records)) => Pair(tableName, records)
        case None => !!!
      }
    override def to(p: Rep[(String, ArrayBuffer[R])]) = {
      val Pair(tableName, records) = p
      ReadWriteTable(tableName, records)
    }
    lazy val tag = {
      weakTypeTag[ReadWriteTable[R]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[ReadWriteTable[R]]](ReadWriteTable("", element[ArrayBuffer[R]].defaultRepValue))
    lazy val eTo = new ReadWriteTableElem[R](this)
  }
  // 4) constructor and deconstructor
  abstract class ReadWriteTableCompanionAbs extends CompanionBase[ReadWriteTableCompanionAbs] with ReadWriteTableCompanion {
    override def toString = "ReadWriteTable"
    def apply[R](p: Rep[ReadWriteTableData[R]])(implicit schema: Elem[R]): Rep[ReadWriteTable[R]] =
      isoReadWriteTable(schema).to(p)
    def apply[R](tableName: Rep[String], records: Rep[ArrayBuffer[R]])(implicit schema: Elem[R]): Rep[ReadWriteTable[R]] =
      mkReadWriteTable(tableName, records)
    def unapply[R:Elem](p: Rep[ReadWriteTable[R]]) = unmkReadWriteTable(p)
  }
  def ReadWriteTable: Rep[ReadWriteTableCompanionAbs]
  implicit def proxyReadWriteTableCompanion(p: Rep[ReadWriteTableCompanionAbs]): ReadWriteTableCompanionAbs = {
    proxyOps[ReadWriteTableCompanionAbs](p)
  }

  class ReadWriteTableCompanionElem extends CompanionElem[ReadWriteTableCompanionAbs] {
    lazy val tag = weakTypeTag[ReadWriteTableCompanionAbs]
    protected def getDefaultRep = ReadWriteTable
  }
  implicit lazy val ReadWriteTableCompanionElem: ReadWriteTableCompanionElem = new ReadWriteTableCompanionElem

  implicit def proxyReadWriteTable[R](p: Rep[ReadWriteTable[R]]): ReadWriteTable[R] =
    proxyOps[ReadWriteTable[R]](p)

  implicit class ExtendedReadWriteTable[R](p: Rep[ReadWriteTable[R]])(implicit schema: Elem[R]) {
    def toData: Rep[ReadWriteTableData[R]] = isoReadWriteTable(schema).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoReadWriteTable[R](implicit schema: Elem[R]): Iso[ReadWriteTableData[R], ReadWriteTable[R]] =
    new ReadWriteTableIso[R]

  // 6) smart constructor and deconstructor
  def mkReadWriteTable[R](tableName: Rep[String], records: Rep[ArrayBuffer[R]])(implicit schema: Elem[R]): Rep[ReadWriteTable[R]]
  def unmkReadWriteTable[R:Elem](p: Rep[ReadWriteTable[R]]): Option[(Rep[String], Rep[ArrayBuffer[R]])]

  // elem for concrete class
  class ReadOnlyTableElem[R](iso: Iso[ReadOnlyTableData[R], ReadOnlyTable[R]]) extends TableElem[R, ReadOnlyTableData[R], ReadOnlyTable[R]](iso)

  // state representation type
  type ReadOnlyTableData[R] = Array[R]

  // 3) Iso for concrete class
  class ReadOnlyTableIso[R](implicit schema: Elem[R])
    extends Iso[ReadOnlyTableData[R], ReadOnlyTable[R]] {
    override def from(p: Rep[ReadOnlyTable[R]]) =
      unmkReadOnlyTable(p) match {
        case Some((records)) => records
        case None => !!!
      }
    override def to(p: Rep[Array[R]]) = {
      val records = p
      ReadOnlyTable(records)
    }
    lazy val tag = {
      weakTypeTag[ReadOnlyTable[R]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[ReadOnlyTable[R]]](ReadOnlyTable(element[Array[R]].defaultRepValue))
    lazy val eTo = new ReadOnlyTableElem[R](this)
  }
  // 4) constructor and deconstructor
  abstract class ReadOnlyTableCompanionAbs extends CompanionBase[ReadOnlyTableCompanionAbs] with ReadOnlyTableCompanion {
    override def toString = "ReadOnlyTable"

    def apply[R](records: Rep[Array[R]])(implicit schema: Elem[R]): Rep[ReadOnlyTable[R]] =
      mkReadOnlyTable(records)
    def unapply[R:Elem](p: Rep[ReadOnlyTable[R]]) = unmkReadOnlyTable(p)
  }
  def ReadOnlyTable: Rep[ReadOnlyTableCompanionAbs]
  implicit def proxyReadOnlyTableCompanion(p: Rep[ReadOnlyTableCompanionAbs]): ReadOnlyTableCompanionAbs = {
    proxyOps[ReadOnlyTableCompanionAbs](p)
  }

  class ReadOnlyTableCompanionElem extends CompanionElem[ReadOnlyTableCompanionAbs] {
    lazy val tag = weakTypeTag[ReadOnlyTableCompanionAbs]
    protected def getDefaultRep = ReadOnlyTable
  }
  implicit lazy val ReadOnlyTableCompanionElem: ReadOnlyTableCompanionElem = new ReadOnlyTableCompanionElem

  implicit def proxyReadOnlyTable[R](p: Rep[ReadOnlyTable[R]]): ReadOnlyTable[R] =
    proxyOps[ReadOnlyTable[R]](p)

  implicit class ExtendedReadOnlyTable[R](p: Rep[ReadOnlyTable[R]])(implicit schema: Elem[R]) {
    def toData: Rep[ReadOnlyTableData[R]] = isoReadOnlyTable(schema).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoReadOnlyTable[R](implicit schema: Elem[R]): Iso[ReadOnlyTableData[R], ReadOnlyTable[R]] =
    new ReadOnlyTableIso[R]

  // 6) smart constructor and deconstructor
  def mkReadOnlyTable[R](records: Rep[Array[R]])(implicit schema: Elem[R]): Rep[ReadOnlyTable[R]]
  def unmkReadOnlyTable[R:Elem](p: Rep[ReadOnlyTable[R]]): Option[(Rep[Array[R]])]

  // elem for concrete class
  class PairTableElem[R1, R2](iso: Iso[PairTableData[R1, R2], PairTable[R1, R2]]) extends TableElem[(R1,R2), PairTableData[R1, R2], PairTable[R1, R2]](iso)

  // state representation type
  type PairTableData[R1, R2] = (Table[R1], Table[R2])

  // 3) Iso for concrete class
  class PairTableIso[R1, R2](implicit leftSchema: Elem[R1], rightSchema: Elem[R2])
    extends Iso[PairTableData[R1, R2], PairTable[R1, R2]] {
    override def from(p: Rep[PairTable[R1, R2]]) =
      unmkPairTable(p) match {
        case Some((left, right)) => Pair(left, right)
        case None => !!!
      }
    override def to(p: Rep[(Table[R1], Table[R2])]) = {
      val Pair(left, right) = p
      PairTable(left, right)
    }
    lazy val tag = {
      weakTypeTag[PairTable[R1, R2]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[PairTable[R1, R2]]](PairTable(element[Table[R1]].defaultRepValue, element[Table[R2]].defaultRepValue))
    lazy val eTo = new PairTableElem[R1, R2](this)
  }
  // 4) constructor and deconstructor
  abstract class PairTableCompanionAbs extends CompanionBase[PairTableCompanionAbs] with PairTableCompanion {
    override def toString = "PairTable"
    def apply[R1, R2](p: Rep[PairTableData[R1, R2]])(implicit leftSchema: Elem[R1], rightSchema: Elem[R2]): Rep[PairTable[R1, R2]] =
      isoPairTable(leftSchema, rightSchema).to(p)
    def apply[R1, R2](left: Rep[Table[R1]], right: Rep[Table[R2]])(implicit leftSchema: Elem[R1], rightSchema: Elem[R2]): Rep[PairTable[R1, R2]] =
      mkPairTable(left, right)
    def unapply[R1:Elem, R2:Elem](p: Rep[PairTable[R1, R2]]) = unmkPairTable(p)
  }
  def PairTable: Rep[PairTableCompanionAbs]
  implicit def proxyPairTableCompanion(p: Rep[PairTableCompanionAbs]): PairTableCompanionAbs = {
    proxyOps[PairTableCompanionAbs](p)
  }

  class PairTableCompanionElem extends CompanionElem[PairTableCompanionAbs] {
    lazy val tag = weakTypeTag[PairTableCompanionAbs]
    protected def getDefaultRep = PairTable
  }
  implicit lazy val PairTableCompanionElem: PairTableCompanionElem = new PairTableCompanionElem

  implicit def proxyPairTable[R1, R2](p: Rep[PairTable[R1, R2]]): PairTable[R1, R2] =
    proxyOps[PairTable[R1, R2]](p)

  implicit class ExtendedPairTable[R1, R2](p: Rep[PairTable[R1, R2]])(implicit leftSchema: Elem[R1], rightSchema: Elem[R2]) {
    def toData: Rep[PairTableData[R1, R2]] = isoPairTable(leftSchema, rightSchema).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoPairTable[R1, R2](implicit leftSchema: Elem[R1], rightSchema: Elem[R2]): Iso[PairTableData[R1, R2], PairTable[R1, R2]] =
    new PairTableIso[R1, R2]

  // 6) smart constructor and deconstructor
  def mkPairTable[R1, R2](left: Rep[Table[R1]], right: Rep[Table[R2]])(implicit leftSchema: Elem[R1], rightSchema: Elem[R2]): Rep[PairTable[R1, R2]]
  def unmkPairTable[R1:Elem, R2:Elem](p: Rep[PairTable[R1, R2]]): Option[(Rep[Table[R1]], Rep[Table[R2]])]

  // elem for concrete class
  class ShardedTableElem[R](iso: Iso[ShardedTableData[R], ShardedTable[R]]) extends TableElem[R, ShardedTableData[R], ShardedTable[R]](iso)

  // state representation type
  type ShardedTableData[R] = (String, (Int, (R => Int, Array[Table[R]])))

  // 3) Iso for concrete class
  class ShardedTableIso[R](implicit schema: Elem[R], shardKeyPath: String)
    extends Iso[ShardedTableData[R], ShardedTable[R]] {
    override def from(p: Rep[ShardedTable[R]]) =
      unmkShardedTable(p) match {
        case Some((tableName, nShards, distrib, shards)) => Pair(tableName, Pair(nShards, Pair(distrib, shards)))
        case None => !!!
      }
    override def to(p: Rep[(String, (Int, (R => Int, Array[Table[R]])))]) = {
      val Pair(tableName, Pair(nShards, Pair(distrib, shards))) = p
      ShardedTable(tableName, nShards, distrib, shards)
    }
    lazy val tag = {
      weakTypeTag[ShardedTable[R]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[ShardedTable[R]]](ShardedTable("", 0, fun { (x: Rep[R]) => 0 }, element[Array[Table[R]]].defaultRepValue))
    lazy val eTo = new ShardedTableElem[R](this)
  }
  // 4) constructor and deconstructor
  abstract class ShardedTableCompanionAbs extends CompanionBase[ShardedTableCompanionAbs] with ShardedTableCompanion {
    override def toString = "ShardedTable"
    def apply[R](p: Rep[ShardedTableData[R]])(implicit schema: Elem[R], shardKeyPath: String): Rep[ShardedTable[R]] =
      isoShardedTable(schema, shardKeyPath).to(p)
    def apply[R](tableName: Rep[String], nShards: Rep[Int], distrib: Rep[R => Int], shards: Rep[Array[Table[R]]])(implicit schema: Elem[R], shardKeyPath: String): Rep[ShardedTable[R]] =
      mkShardedTable(tableName, nShards, distrib, shards)
    def unapply[R:Elem](p: Rep[ShardedTable[R]]) = unmkShardedTable(p)
  }
  def ShardedTable: Rep[ShardedTableCompanionAbs]
  implicit def proxyShardedTableCompanion(p: Rep[ShardedTableCompanionAbs]): ShardedTableCompanionAbs = {
    proxyOps[ShardedTableCompanionAbs](p)
  }

  class ShardedTableCompanionElem extends CompanionElem[ShardedTableCompanionAbs] {
    lazy val tag = weakTypeTag[ShardedTableCompanionAbs]
    protected def getDefaultRep = ShardedTable
  }
  implicit lazy val ShardedTableCompanionElem: ShardedTableCompanionElem = new ShardedTableCompanionElem

  implicit def proxyShardedTable[R](p: Rep[ShardedTable[R]]): ShardedTable[R] =
    proxyOps[ShardedTable[R]](p)

  implicit class ExtendedShardedTable[R](p: Rep[ShardedTable[R]])(implicit schema: Elem[R], shardKeyPath: String) {
    def toData: Rep[ShardedTableData[R]] = isoShardedTable(schema, shardKeyPath).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoShardedTable[R](implicit schema: Elem[R], shardKeyPath: String): Iso[ShardedTableData[R], ShardedTable[R]] =
    new ShardedTableIso[R]

  // 6) smart constructor and deconstructor
  def mkShardedTable[R](tableName: Rep[String], nShards: Rep[Int], distrib: Rep[R => Int], shards: Rep[Array[Table[R]]])(implicit schema: Elem[R], shardKeyPath: String): Rep[ShardedTable[R]]
  def unmkShardedTable[R:Elem](p: Rep[ShardedTable[R]]): Option[(Rep[String], Rep[Int], Rep[R => Int], Rep[Array[Table[R]]])]

  // elem for concrete class
  class ShardedViewElem[R](iso: Iso[ShardedViewData[R], ShardedView[R]]) extends TableElem[R, ShardedViewData[R], ShardedView[R]](iso)

  // state representation type
  type ShardedViewData[R] = (Int, Int => Table[R])

  // 3) Iso for concrete class
  class ShardedViewIso[R](implicit schema: Elem[R], shardKeyPath: String)
    extends Iso[ShardedViewData[R], ShardedView[R]] {
    override def from(p: Rep[ShardedView[R]]) =
      unmkShardedView(p) match {
        case Some((nShards, view)) => Pair(nShards, view)
        case None => !!!
      }
    override def to(p: Rep[(Int, Int => Table[R])]) = {
      val Pair(nShards, view) = p
      ShardedView(nShards, view)
    }
    lazy val tag = {
      weakTypeTag[ShardedView[R]]
    }
    lazy val defaultRepTo = Default.defaultVal[Rep[ShardedView[R]]](ShardedView(0, fun { (x: Rep[Int]) => element[Table[R]].defaultRepValue }))
    lazy val eTo = new ShardedViewElem[R](this)
  }
  // 4) constructor and deconstructor
  abstract class ShardedViewCompanionAbs extends CompanionBase[ShardedViewCompanionAbs] with ShardedViewCompanion {
    override def toString = "ShardedView"
    def apply[R](p: Rep[ShardedViewData[R]])(implicit schema: Elem[R], shardKeyPath: String): Rep[ShardedView[R]] =
      isoShardedView(schema, shardKeyPath).to(p)
    def apply[R](nShards: Rep[Int], view: Rep[Int => Table[R]])(implicit schema: Elem[R], shardKeyPath: String): Rep[ShardedView[R]] =
      mkShardedView(nShards, view)
    def unapply[R:Elem](p: Rep[ShardedView[R]]) = unmkShardedView(p)
  }
  def ShardedView: Rep[ShardedViewCompanionAbs]
  implicit def proxyShardedViewCompanion(p: Rep[ShardedViewCompanionAbs]): ShardedViewCompanionAbs = {
    proxyOps[ShardedViewCompanionAbs](p)
  }

  class ShardedViewCompanionElem extends CompanionElem[ShardedViewCompanionAbs] {
    lazy val tag = weakTypeTag[ShardedViewCompanionAbs]
    protected def getDefaultRep = ShardedView
  }
  implicit lazy val ShardedViewCompanionElem: ShardedViewCompanionElem = new ShardedViewCompanionElem

  implicit def proxyShardedView[R](p: Rep[ShardedView[R]]): ShardedView[R] =
    proxyOps[ShardedView[R]](p)

  implicit class ExtendedShardedView[R](p: Rep[ShardedView[R]])(implicit schema: Elem[R], shardKeyPath: String) {
    def toData: Rep[ShardedViewData[R]] = isoShardedView(schema, shardKeyPath).from(p)
  }

  // 5) implicit resolution of Iso
  implicit def isoShardedView[R](implicit schema: Elem[R], shardKeyPath: String): Iso[ShardedViewData[R], ShardedView[R]] =
    new ShardedViewIso[R]

  // 6) smart constructor and deconstructor
  def mkShardedView[R](nShards: Rep[Int], view: Rep[Int => Table[R]])(implicit schema: Elem[R], shardKeyPath: String): Rep[ShardedView[R]]
  def unmkShardedView[R:Elem](p: Rep[ShardedView[R]]): Option[(Rep[Int], Rep[Int => Table[R]])]
}

// Seq -----------------------------------
trait SqlSeq extends SqlDsl with ScalanSeq {
  self: SqlDslSeq =>
  lazy val Table: Rep[TableCompanionAbs] = new TableCompanionAbs with UserTypeSeq[TableCompanionAbs, TableCompanionAbs] {
    lazy val selfType = element[TableCompanionAbs]
  }

  case class SeqBaseTable[R]
      (override val tableName: Rep[String])
      (implicit schema: Elem[R])
    extends BaseTable[R](tableName)
        with UserTypeSeq[Table[R], BaseTable[R]] {
    lazy val selfType = element[BaseTable[R]].asInstanceOf[Elem[Table[R]]]
  }
  lazy val BaseTable = new BaseTableCompanionAbs with UserTypeSeq[BaseTableCompanionAbs, BaseTableCompanionAbs] {
    lazy val selfType = element[BaseTableCompanionAbs]
  }

  def mkBaseTable[R]
      (tableName: Rep[String])(implicit schema: Elem[R]) =
      new SeqBaseTable[R](tableName)
  def unmkBaseTable[R:Elem](p: Rep[BaseTable[R]]) =
    Some((p.tableName))

  case class SeqUniqueIndex[K, R]
      (override val tableName: Rep[String], override val table: Rep[Table[R]], override val map: Rep[MMap[K,R]], override val getKey: Rep[R => K])
      (implicit schema: Elem[R], index: Elem[K], keyPath: String)
    extends UniqueIndex[K, R](tableName, table, map, getKey)
        with UserTypeSeq[BaseTable[R], UniqueIndex[K, R]] {
    lazy val selfType = element[UniqueIndex[K, R]].asInstanceOf[Elem[BaseTable[R]]]
  }
  lazy val UniqueIndex = new UniqueIndexCompanionAbs with UserTypeSeq[UniqueIndexCompanionAbs, UniqueIndexCompanionAbs] {
    lazy val selfType = element[UniqueIndexCompanionAbs]
  }

  def mkUniqueIndex[K, R]
      (tableName: Rep[String], table: Rep[Table[R]], map: Rep[MMap[K,R]], getKey: Rep[R => K])(implicit schema: Elem[R], index: Elem[K], keyPath: String) =
      new SeqUniqueIndex[K, R](tableName, table, map, getKey)
  def unmkUniqueIndex[K:Elem, R:Elem](p: Rep[UniqueIndex[K, R]]) =
    Some((p.tableName, p.table, p.map, p.getKey))

  case class SeqNonUniqueIndex[K, R]
      (override val tableName: Rep[String], override val table: Rep[Table[R]], override val map: Rep[MMultiMap[K,R]], override val getKey: Rep[R => K])
      (implicit schema: Elem[R], index: Elem[K], keyPath: String)
    extends NonUniqueIndex[K, R](tableName, table, map, getKey)
        with UserTypeSeq[BaseTable[R], NonUniqueIndex[K, R]] {
    lazy val selfType = element[NonUniqueIndex[K, R]].asInstanceOf[Elem[BaseTable[R]]]
  }
  lazy val NonUniqueIndex = new NonUniqueIndexCompanionAbs with UserTypeSeq[NonUniqueIndexCompanionAbs, NonUniqueIndexCompanionAbs] {
    lazy val selfType = element[NonUniqueIndexCompanionAbs]
  }

  def mkNonUniqueIndex[K, R]
      (tableName: Rep[String], table: Rep[Table[R]], map: Rep[MMultiMap[K,R]], getKey: Rep[R => K])(implicit schema: Elem[R], index: Elem[K], keyPath: String) =
      new SeqNonUniqueIndex[K, R](tableName, table, map, getKey)
  def unmkNonUniqueIndex[K:Elem, R:Elem](p: Rep[NonUniqueIndex[K, R]]) =
    Some((p.tableName, p.table, p.map, p.getKey))

  case class SeqReadWriteTable[R]
      (override val tableName: Rep[String], override val records: Rep[ArrayBuffer[R]])
      (implicit schema: Elem[R])
    extends ReadWriteTable[R](tableName, records)
        with UserTypeSeq[BaseTable[R], ReadWriteTable[R]] {
    lazy val selfType = element[ReadWriteTable[R]].asInstanceOf[Elem[BaseTable[R]]]
  }
  lazy val ReadWriteTable = new ReadWriteTableCompanionAbs with UserTypeSeq[ReadWriteTableCompanionAbs, ReadWriteTableCompanionAbs] {
    lazy val selfType = element[ReadWriteTableCompanionAbs]
  }

  def mkReadWriteTable[R]
      (tableName: Rep[String], records: Rep[ArrayBuffer[R]])(implicit schema: Elem[R]) =
      new SeqReadWriteTable[R](tableName, records)
  def unmkReadWriteTable[R:Elem](p: Rep[ReadWriteTable[R]]) =
    Some((p.tableName, p.records))

  case class SeqReadOnlyTable[R]
      (override val records: Rep[Array[R]])
      (implicit schema: Elem[R])
    extends ReadOnlyTable[R](records)
        with UserTypeSeq[BaseTable[R], ReadOnlyTable[R]] {
    lazy val selfType = element[ReadOnlyTable[R]].asInstanceOf[Elem[BaseTable[R]]]
  }
  lazy val ReadOnlyTable = new ReadOnlyTableCompanionAbs with UserTypeSeq[ReadOnlyTableCompanionAbs, ReadOnlyTableCompanionAbs] {
    lazy val selfType = element[ReadOnlyTableCompanionAbs]
  }

  def mkReadOnlyTable[R]
      (records: Rep[Array[R]])(implicit schema: Elem[R]) =
      new SeqReadOnlyTable[R](records)
  def unmkReadOnlyTable[R:Elem](p: Rep[ReadOnlyTable[R]]) =
    Some((p.records))

  case class SeqPairTable[R1, R2]
      (override val left: Rep[Table[R1]], override val right: Rep[Table[R2]])
      (implicit leftSchema: Elem[R1], rightSchema: Elem[R2])
    extends PairTable[R1, R2](left, right)
        with UserTypeSeq[BaseTable[(R1,R2)], PairTable[R1, R2]] {
    lazy val selfType = element[PairTable[R1, R2]].asInstanceOf[Elem[BaseTable[(R1,R2)]]]
  }
  lazy val PairTable = new PairTableCompanionAbs with UserTypeSeq[PairTableCompanionAbs, PairTableCompanionAbs] {
    lazy val selfType = element[PairTableCompanionAbs]
  }

  def mkPairTable[R1, R2]
      (left: Rep[Table[R1]], right: Rep[Table[R2]])(implicit leftSchema: Elem[R1], rightSchema: Elem[R2]) =
      new SeqPairTable[R1, R2](left, right)
  def unmkPairTable[R1:Elem, R2:Elem](p: Rep[PairTable[R1, R2]]) =
    Some((p.left, p.right))

  case class SeqShardedTable[R]
      (override val tableName: Rep[String], override val nShards: Rep[Int], override val distrib: Rep[R => Int], override val shards: Rep[Array[Table[R]]])
      (implicit schema: Elem[R], shardKeyPath: String)
    extends ShardedTable[R](tableName, nShards, distrib, shards)
        with UserTypeSeq[BaseTable[R], ShardedTable[R]] {
    lazy val selfType = element[ShardedTable[R]].asInstanceOf[Elem[BaseTable[R]]]
  }
  lazy val ShardedTable = new ShardedTableCompanionAbs with UserTypeSeq[ShardedTableCompanionAbs, ShardedTableCompanionAbs] {
    lazy val selfType = element[ShardedTableCompanionAbs]
  }

  def mkShardedTable[R]
      (tableName: Rep[String], nShards: Rep[Int], distrib: Rep[R => Int], shards: Rep[Array[Table[R]]])(implicit schema: Elem[R], shardKeyPath: String) =
      new SeqShardedTable[R](tableName, nShards, distrib, shards)
  def unmkShardedTable[R:Elem](p: Rep[ShardedTable[R]]) =
    Some((p.tableName, p.nShards, p.distrib, p.shards))

  case class SeqShardedView[R]
      (override val nShards: Rep[Int], override val view: Rep[Int => Table[R]])
      (implicit schema: Elem[R], shardKeyPath: String)
    extends ShardedView[R](nShards, view)
        with UserTypeSeq[BaseTable[R], ShardedView[R]] {
    lazy val selfType = element[ShardedView[R]].asInstanceOf[Elem[BaseTable[R]]]
  }
  lazy val ShardedView = new ShardedViewCompanionAbs with UserTypeSeq[ShardedViewCompanionAbs, ShardedViewCompanionAbs] {
    lazy val selfType = element[ShardedViewCompanionAbs]
  }

  def mkShardedView[R]
      (nShards: Rep[Int], view: Rep[Int => Table[R]])(implicit schema: Elem[R], shardKeyPath: String) =
      new SeqShardedView[R](nShards, view)
  def unmkShardedView[R:Elem](p: Rep[ShardedView[R]]) =
    Some((p.nShards, p.view))
}

// Exp -----------------------------------
trait SqlExp extends SqlDsl with ScalanExp {
  self: SqlDslExp =>
  lazy val Table: Rep[TableCompanionAbs] = new TableCompanionAbs with UserTypeDef[TableCompanionAbs, TableCompanionAbs] {
    lazy val selfType = element[TableCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  case class ExpBaseTable[R]
      (override val tableName: Rep[String])
      (implicit schema: Elem[R])
    extends BaseTable[R](tableName) with UserTypeDef[Table[R], BaseTable[R]] {
    lazy val selfType = element[BaseTable[R]].asInstanceOf[Elem[Table[R]]]
    override def mirror(t: Transformer) = ExpBaseTable[R](t(tableName))
  }

  lazy val BaseTable: Rep[BaseTableCompanionAbs] = new BaseTableCompanionAbs with UserTypeDef[BaseTableCompanionAbs, BaseTableCompanionAbs] {
    lazy val selfType = element[BaseTableCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object BaseTableMethods {
    object PrimaryKey {
      def unapply(d: Def[_]): Option[(Rep[BaseTable[R]], Rep[R => K]) forSome {type R; type K}] = d match {
        case MethodCall(receiver, method, Seq(key, _*), _) if receiver.elem.isInstanceOf[BaseTableElem[_]] && method.getName == "PrimaryKey" =>
          Some((receiver, key)).asInstanceOf[Option[(Rep[BaseTable[R]], Rep[R => K]) forSome {type R; type K}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[BaseTable[R]], Rep[R => K]) forSome {type R; type K}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object SecondaryKey {
      def unapply(d: Def[_]): Option[(Rep[BaseTable[R]], Rep[R => K]) forSome {type R; type K}] = d match {
        case MethodCall(receiver, method, Seq(key, _*), _) if receiver.elem.isInstanceOf[BaseTableElem[_]] && method.getName == "SecondaryKey" =>
          Some((receiver, key)).asInstanceOf[Option[(Rep[BaseTable[R]], Rep[R => K]) forSome {type R; type K}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[BaseTable[R]], Rep[R => K]) forSome {type R; type K}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object toArray {
      def unapply(d: Def[_]): Option[Rep[BaseTable[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[BaseTableElem[_]] && method.getName == "toArray" =>
          Some(receiver).asInstanceOf[Option[Rep[BaseTable[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[BaseTable[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insert {
      def unapply(d: Def[_]): Option[(Rep[BaseTable[R]], Rep[R]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(record, _*), _) if receiver.elem.isInstanceOf[BaseTableElem[_]] && method.getName == "insert" =>
          Some((receiver, record)).asInstanceOf[Option[(Rep[BaseTable[R]], Rep[R]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[BaseTable[R]], Rep[R]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object BaseTableCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[BaseTableCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkBaseTable[R]
    (tableName: Rep[String])(implicit schema: Elem[R]) =
    new ExpBaseTable[R](tableName)
  def unmkBaseTable[R:Elem](p: Rep[BaseTable[R]]) =
    Some((p.tableName))

  case class ExpUniqueIndex[K, R]
      (override val tableName: Rep[String], override val table: Rep[Table[R]], override val map: Rep[MMap[K,R]], override val getKey: Rep[R => K])
      (implicit schema: Elem[R], index: Elem[K], keyPath: String)
    extends UniqueIndex[K, R](tableName, table, map, getKey) with UserTypeDef[BaseTable[R], UniqueIndex[K, R]] {
    lazy val selfType = element[UniqueIndex[K, R]].asInstanceOf[Elem[BaseTable[R]]]
    override def mirror(t: Transformer) = ExpUniqueIndex[K, R](t(tableName), t(table), t(map), t(getKey))
  }

  lazy val UniqueIndex: Rep[UniqueIndexCompanionAbs] = new UniqueIndexCompanionAbs with UserTypeDef[UniqueIndexCompanionAbs, UniqueIndexCompanionAbs] {
    lazy val selfType = element[UniqueIndexCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object UniqueIndexMethods {
    object filter {
      def unapply(d: Def[_]): Option[(Rep[UniqueIndex[K, R]], Rep[R => Boolean]) forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, Seq(predicate, _*), _) if receiver.elem.isInstanceOf[UniqueIndexElem[_, _]] && method.getName == "filter" =>
          Some((receiver, predicate)).asInstanceOf[Option[(Rep[UniqueIndex[K, R]], Rep[R => Boolean]) forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[UniqueIndex[K, R]], Rep[R => Boolean]) forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object count {
      def unapply(d: Def[_]): Option[Rep[UniqueIndex[K, R]] forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[UniqueIndexElem[_, _]] && method.getName == "count" =>
          Some(receiver).asInstanceOf[Option[Rep[UniqueIndex[K, R]] forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[UniqueIndex[K, R]] forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insert {
      def unapply(d: Def[_]): Option[(Rep[UniqueIndex[K, R]], Rep[R]) forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, Seq(record, _*), _) if receiver.elem.isInstanceOf[UniqueIndexElem[_, _]] && method.getName == "insert" =>
          Some((receiver, record)).asInstanceOf[Option[(Rep[UniqueIndex[K, R]], Rep[R]) forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[UniqueIndex[K, R]], Rep[R]) forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Join {
      def unapply(d: Def[_]): Option[(Rep[UniqueIndex[K, R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type K; type R; type I}] = d match {
        case MethodCall(receiver, method, Seq(inner, outKey, inKey, _*), _) if receiver.elem.isInstanceOf[UniqueIndexElem[_, _]] && method.getName == "Join" =>
          Some((receiver, inner, outKey, inKey)).asInstanceOf[Option[(Rep[UniqueIndex[K, R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type K; type R; type I}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[UniqueIndex[K, R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type K; type R; type I}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object toArray {
      def unapply(d: Def[_]): Option[Rep[UniqueIndex[K, R]] forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[UniqueIndexElem[_, _]] && method.getName == "toArray" =>
          Some(receiver).asInstanceOf[Option[Rep[UniqueIndex[K, R]] forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[UniqueIndex[K, R]] forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object UniqueIndexCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[UniqueIndexCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[(Rep[String], Rep[Table[R]], Rep[R => K]) forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, Seq(tableName, table, key, _*), _) if receiver.elem.isInstanceOf[UniqueIndexCompanionElem] && method.getName == "create" =>
          Some((tableName, table, key)).asInstanceOf[Option[(Rep[String], Rep[Table[R]], Rep[R => K]) forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[String], Rep[Table[R]], Rep[R => K]) forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkUniqueIndex[K, R]
    (tableName: Rep[String], table: Rep[Table[R]], map: Rep[MMap[K,R]], getKey: Rep[R => K])(implicit schema: Elem[R], index: Elem[K], keyPath: String) =
    new ExpUniqueIndex[K, R](tableName, table, map, getKey)
  def unmkUniqueIndex[K:Elem, R:Elem](p: Rep[UniqueIndex[K, R]]) =
    Some((p.tableName, p.table, p.map, p.getKey))

  case class ExpNonUniqueIndex[K, R]
      (override val tableName: Rep[String], override val table: Rep[Table[R]], override val map: Rep[MMultiMap[K,R]], override val getKey: Rep[R => K])
      (implicit schema: Elem[R], index: Elem[K], keyPath: String)
    extends NonUniqueIndex[K, R](tableName, table, map, getKey) with UserTypeDef[BaseTable[R], NonUniqueIndex[K, R]] {
    lazy val selfType = element[NonUniqueIndex[K, R]].asInstanceOf[Elem[BaseTable[R]]]
    override def mirror(t: Transformer) = ExpNonUniqueIndex[K, R](t(tableName), t(table), t(map), t(getKey))
  }

  lazy val NonUniqueIndex: Rep[NonUniqueIndexCompanionAbs] = new NonUniqueIndexCompanionAbs with UserTypeDef[NonUniqueIndexCompanionAbs, NonUniqueIndexCompanionAbs] {
    lazy val selfType = element[NonUniqueIndexCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object NonUniqueIndexMethods {
    object filter {
      def unapply(d: Def[_]): Option[(Rep[NonUniqueIndex[K, R]], Rep[R => Boolean]) forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, Seq(predicate, _*), _) if receiver.elem.isInstanceOf[NonUniqueIndexElem[_, _]] && method.getName == "filter" =>
          Some((receiver, predicate)).asInstanceOf[Option[(Rep[NonUniqueIndex[K, R]], Rep[R => Boolean]) forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[NonUniqueIndex[K, R]], Rep[R => Boolean]) forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object count {
      def unapply(d: Def[_]): Option[Rep[NonUniqueIndex[K, R]] forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[NonUniqueIndexElem[_, _]] && method.getName == "count" =>
          Some(receiver).asInstanceOf[Option[Rep[NonUniqueIndex[K, R]] forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[NonUniqueIndex[K, R]] forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insert {
      def unapply(d: Def[_]): Option[(Rep[NonUniqueIndex[K, R]], Rep[R]) forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, Seq(record, _*), _) if receiver.elem.isInstanceOf[NonUniqueIndexElem[_, _]] && method.getName == "insert" =>
          Some((receiver, record)).asInstanceOf[Option[(Rep[NonUniqueIndex[K, R]], Rep[R]) forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[NonUniqueIndex[K, R]], Rep[R]) forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Join {
      def unapply(d: Def[_]): Option[(Rep[NonUniqueIndex[K, R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type K; type R; type I}] = d match {
        case MethodCall(receiver, method, Seq(inner, outKey, inKey, _*), _) if receiver.elem.isInstanceOf[NonUniqueIndexElem[_, _]] && method.getName == "Join" =>
          Some((receiver, inner, outKey, inKey)).asInstanceOf[Option[(Rep[NonUniqueIndex[K, R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type K; type R; type I}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[NonUniqueIndex[K, R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type K; type R; type I}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object toArray {
      def unapply(d: Def[_]): Option[Rep[NonUniqueIndex[K, R]] forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[NonUniqueIndexElem[_, _]] && method.getName == "toArray" =>
          Some(receiver).asInstanceOf[Option[Rep[NonUniqueIndex[K, R]] forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[NonUniqueIndex[K, R]] forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object NonUniqueIndexCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[NonUniqueIndexCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[(Rep[String], Rep[Table[R]], Rep[R => K]) forSome {type K; type R}] = d match {
        case MethodCall(receiver, method, Seq(tableName, table, key, _*), _) if receiver.elem.isInstanceOf[NonUniqueIndexCompanionElem] && method.getName == "create" =>
          Some((tableName, table, key)).asInstanceOf[Option[(Rep[String], Rep[Table[R]], Rep[R => K]) forSome {type K; type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[String], Rep[Table[R]], Rep[R => K]) forSome {type K; type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkNonUniqueIndex[K, R]
    (tableName: Rep[String], table: Rep[Table[R]], map: Rep[MMultiMap[K,R]], getKey: Rep[R => K])(implicit schema: Elem[R], index: Elem[K], keyPath: String) =
    new ExpNonUniqueIndex[K, R](tableName, table, map, getKey)
  def unmkNonUniqueIndex[K:Elem, R:Elem](p: Rep[NonUniqueIndex[K, R]]) =
    Some((p.tableName, p.table, p.map, p.getKey))

  case class ExpReadWriteTable[R]
      (override val tableName: Rep[String], override val records: Rep[ArrayBuffer[R]])
      (implicit schema: Elem[R])
    extends ReadWriteTable[R](tableName, records) with UserTypeDef[BaseTable[R], ReadWriteTable[R]] {
    lazy val selfType = element[ReadWriteTable[R]].asInstanceOf[Elem[BaseTable[R]]]
    override def mirror(t: Transformer) = ExpReadWriteTable[R](t(tableName), t(records))
  }

  lazy val ReadWriteTable: Rep[ReadWriteTableCompanionAbs] = new ReadWriteTableCompanionAbs with UserTypeDef[ReadWriteTableCompanionAbs, ReadWriteTableCompanionAbs] {
    lazy val selfType = element[ReadWriteTableCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object ReadWriteTableMethods {
    object count {
      def unapply(d: Def[_]): Option[Rep[ReadWriteTable[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ReadWriteTableElem[_]] && method.getName == "count" =>
          Some(receiver).asInstanceOf[Option[Rep[ReadWriteTable[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ReadWriteTable[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insert {
      def unapply(d: Def[_]): Option[(Rep[ReadWriteTable[R]], Rep[R]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(record, _*), _) if receiver.elem.isInstanceOf[ReadWriteTableElem[_]] && method.getName == "insert" =>
          Some((receiver, record)).asInstanceOf[Option[(Rep[ReadWriteTable[R]], Rep[R]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ReadWriteTable[R]], Rep[R]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object toArray {
      def unapply(d: Def[_]): Option[Rep[ReadWriteTable[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ReadWriteTableElem[_]] && method.getName == "toArray" =>
          Some(receiver).asInstanceOf[Option[Rep[ReadWriteTable[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ReadWriteTable[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object ReadWriteTableCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ReadWriteTableCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[Rep[String] forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(tableName, _*), _) if receiver.elem.isInstanceOf[ReadWriteTableCompanionElem] && method.getName == "create" =>
          Some(tableName).asInstanceOf[Option[Rep[String] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[String] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object createShard {
      def unapply(d: Def[_]): Option[Rep[Int] forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(shardNo, _*), _) if receiver.elem.isInstanceOf[ReadWriteTableCompanionElem] && method.getName == "createShard" =>
          Some(shardNo).asInstanceOf[Option[Rep[Int] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Int] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkReadWriteTable[R]
    (tableName: Rep[String], records: Rep[ArrayBuffer[R]])(implicit schema: Elem[R]) =
    new ExpReadWriteTable[R](tableName, records)
  def unmkReadWriteTable[R:Elem](p: Rep[ReadWriteTable[R]]) =
    Some((p.tableName, p.records))

  case class ExpReadOnlyTable[R]
      (override val records: Rep[Array[R]])
      (implicit schema: Elem[R])
    extends ReadOnlyTable[R](records) with UserTypeDef[BaseTable[R], ReadOnlyTable[R]] {
    lazy val selfType = element[ReadOnlyTable[R]].asInstanceOf[Elem[BaseTable[R]]]
    override def mirror(t: Transformer) = ExpReadOnlyTable[R](t(records))
  }

  lazy val ReadOnlyTable: Rep[ReadOnlyTableCompanionAbs] = new ReadOnlyTableCompanionAbs with UserTypeDef[ReadOnlyTableCompanionAbs, ReadOnlyTableCompanionAbs] {
    lazy val selfType = element[ReadOnlyTableCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object ReadOnlyTableMethods {
    object count {
      def unapply(d: Def[_]): Option[Rep[ReadOnlyTable[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ReadOnlyTableElem[_]] && method.getName == "count" =>
          Some(receiver).asInstanceOf[Option[Rep[ReadOnlyTable[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ReadOnlyTable[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insert {
      def unapply(d: Def[_]): Option[(Rep[ReadOnlyTable[R]], Rep[R]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(record, _*), _) if receiver.elem.isInstanceOf[ReadOnlyTableElem[_]] && method.getName == "insert" =>
          Some((receiver, record)).asInstanceOf[Option[(Rep[ReadOnlyTable[R]], Rep[R]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ReadOnlyTable[R]], Rep[R]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object PrimaryKey {
      def unapply(d: Def[_]): Option[(Rep[ReadOnlyTable[R]], Rep[R => K]) forSome {type R; type K}] = d match {
        case MethodCall(receiver, method, Seq(key, _*), _) if receiver.elem.isInstanceOf[ReadOnlyTableElem[_]] && method.getName == "PrimaryKey" =>
          Some((receiver, key)).asInstanceOf[Option[(Rep[ReadOnlyTable[R]], Rep[R => K]) forSome {type R; type K}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ReadOnlyTable[R]], Rep[R => K]) forSome {type R; type K}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object SecondaryKey {
      def unapply(d: Def[_]): Option[(Rep[ReadOnlyTable[R]], Rep[R => K]) forSome {type R; type K}] = d match {
        case MethodCall(receiver, method, Seq(key, _*), _) if receiver.elem.isInstanceOf[ReadOnlyTableElem[_]] && method.getName == "SecondaryKey" =>
          Some((receiver, key)).asInstanceOf[Option[(Rep[ReadOnlyTable[R]], Rep[R => K]) forSome {type R; type K}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ReadOnlyTable[R]], Rep[R => K]) forSome {type R; type K}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object toArray {
      def unapply(d: Def[_]): Option[Rep[ReadOnlyTable[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ReadOnlyTableElem[_]] && method.getName == "toArray" =>
          Some(receiver).asInstanceOf[Option[Rep[ReadOnlyTable[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ReadOnlyTable[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object ReadOnlyTableCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ReadOnlyTableCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkReadOnlyTable[R]
    (records: Rep[Array[R]])(implicit schema: Elem[R]) =
    new ExpReadOnlyTable[R](records)
  def unmkReadOnlyTable[R:Elem](p: Rep[ReadOnlyTable[R]]) =
    Some((p.records))

  case class ExpPairTable[R1, R2]
      (override val left: Rep[Table[R1]], override val right: Rep[Table[R2]])
      (implicit leftSchema: Elem[R1], rightSchema: Elem[R2])
    extends PairTable[R1, R2](left, right) with UserTypeDef[BaseTable[(R1,R2)], PairTable[R1, R2]] {
    lazy val selfType = element[PairTable[R1, R2]].asInstanceOf[Elem[BaseTable[(R1,R2)]]]
    override def mirror(t: Transformer) = ExpPairTable[R1, R2](t(left), t(right))
  }

  lazy val PairTable: Rep[PairTableCompanionAbs] = new PairTableCompanionAbs with UserTypeDef[PairTableCompanionAbs, PairTableCompanionAbs] {
    lazy val selfType = element[PairTableCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object PairTableMethods {
    object count {
      def unapply(d: Def[_]): Option[Rep[PairTable[R1, R2]] forSome {type R1; type R2}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairTableElem[_, _]] && method.getName == "count" =>
          Some(receiver).asInstanceOf[Option[Rep[PairTable[R1, R2]] forSome {type R1; type R2}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairTable[R1, R2]] forSome {type R1; type R2}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insert {
      def unapply(d: Def[_]): Option[(Rep[PairTable[R1, R2]], Rep[(R1,R2)]) forSome {type R1; type R2}] = d match {
        case MethodCall(receiver, method, Seq(record, _*), _) if receiver.elem.isInstanceOf[PairTableElem[_, _]] && method.getName == "insert" =>
          Some((receiver, record)).asInstanceOf[Option[(Rep[PairTable[R1, R2]], Rep[(R1,R2)]) forSome {type R1; type R2}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairTable[R1, R2]], Rep[(R1,R2)]) forSome {type R1; type R2}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insertFrom {
      def unapply(d: Def[_]): Option[(Rep[PairTable[R1, R2]], Arr[(R1,R2)]) forSome {type R1; type R2}] = d match {
        case MethodCall(receiver, method, Seq(arr, _*), _) if receiver.elem.isInstanceOf[PairTableElem[_, _]] && method.getName == "insertFrom" =>
          Some((receiver, arr)).asInstanceOf[Option[(Rep[PairTable[R1, R2]], Arr[(R1,R2)]) forSome {type R1; type R2}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[PairTable[R1, R2]], Arr[(R1,R2)]) forSome {type R1; type R2}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object toArray {
      def unapply(d: Def[_]): Option[Rep[PairTable[R1, R2]] forSome {type R1; type R2}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[PairTableElem[_, _]] && method.getName == "toArray" =>
          Some(receiver).asInstanceOf[Option[Rep[PairTable[R1, R2]] forSome {type R1; type R2}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[PairTable[R1, R2]] forSome {type R1; type R2}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object PairTableCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[(Elem[R1], Elem[R2]) forSome {type R1; type R2}] = d match {
        case MethodCall(receiver, method, Seq(leftSchema, rightSchema, _*), _) if receiver.elem.isInstanceOf[PairTableCompanionElem] && method.getName == "defaultOf" =>
          Some((leftSchema, rightSchema)).asInstanceOf[Option[(Elem[R1], Elem[R2]) forSome {type R1; type R2}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Elem[R1], Elem[R2]) forSome {type R1; type R2}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[(Rep[Table[R1]], Rep[Table[R2]], Elem[R1], Elem[R2]) forSome {type R1; type R2}] = d match {
        case MethodCall(receiver, method, Seq(l, r, leftSchema, rightSchema, _*), _) if receiver.elem.isInstanceOf[PairTableCompanionElem] && method.getName == "create" =>
          Some((l, r, leftSchema, rightSchema)).asInstanceOf[Option[(Rep[Table[R1]], Rep[Table[R2]], Elem[R1], Elem[R2]) forSome {type R1; type R2}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R1]], Rep[Table[R2]], Elem[R1], Elem[R2]) forSome {type R1; type R2}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkPairTable[R1, R2]
    (left: Rep[Table[R1]], right: Rep[Table[R2]])(implicit leftSchema: Elem[R1], rightSchema: Elem[R2]) =
    new ExpPairTable[R1, R2](left, right)
  def unmkPairTable[R1:Elem, R2:Elem](p: Rep[PairTable[R1, R2]]) =
    Some((p.left, p.right))

  case class ExpShardedTable[R]
      (override val tableName: Rep[String], override val nShards: Rep[Int], override val distrib: Rep[R => Int], override val shards: Rep[Array[Table[R]]])
      (implicit schema: Elem[R], shardKeyPath: String)
    extends ShardedTable[R](tableName, nShards, distrib, shards) with UserTypeDef[BaseTable[R], ShardedTable[R]] {
    lazy val selfType = element[ShardedTable[R]].asInstanceOf[Elem[BaseTable[R]]]
    override def mirror(t: Transformer) = ExpShardedTable[R](t(tableName), t(nShards), t(distrib), t(shards))
  }

  lazy val ShardedTable: Rep[ShardedTableCompanionAbs] = new ShardedTableCompanionAbs with UserTypeDef[ShardedTableCompanionAbs, ShardedTableCompanionAbs] {
    lazy val selfType = element[ShardedTableCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object ShardedTableMethods {
    object count {
      def unapply(d: Def[_]): Option[Rep[ShardedTable[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "count" =>
          Some(receiver).asInstanceOf[Option[Rep[ShardedTable[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ShardedTable[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insert {
      def unapply(d: Def[_]): Option[(Rep[ShardedTable[R]], Rep[R]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(record, _*), _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "insert" =>
          Some((receiver, record)).asInstanceOf[Option[(Rep[ShardedTable[R]], Rep[R]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedTable[R]], Rep[R]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insertFrom {
      def unapply(d: Def[_]): Option[(Rep[ShardedTable[R]], Arr[R]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(arr, _*), _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "insertFrom" =>
          Some((receiver, arr)).asInstanceOf[Option[(Rep[ShardedTable[R]], Arr[R]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedTable[R]], Arr[R]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object filter {
      def unapply(d: Def[_]): Option[(Rep[ShardedTable[R]], Rep[R => Boolean]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(predicate, _*), _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "filter" =>
          Some((receiver, predicate)).asInstanceOf[Option[(Rep[ShardedTable[R]], Rep[R => Boolean]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedTable[R]], Rep[R => Boolean]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Sum {
      def unapply(d: Def[_]): Option[(Rep[ShardedTable[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, n, _*), _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "Sum" =>
          Some((receiver, agg, n)).asInstanceOf[Option[(Rep[ShardedTable[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedTable[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Max {
      def unapply(d: Def[_]): Option[(Rep[ShardedTable[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, o, _*), _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "Max" =>
          Some((receiver, agg, o)).asInstanceOf[Option[(Rep[ShardedTable[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedTable[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Min {
      def unapply(d: Def[_]): Option[(Rep[ShardedTable[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, o, _*), _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "Min" =>
          Some((receiver, agg, o)).asInstanceOf[Option[(Rep[ShardedTable[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedTable[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object MapReduce {
      def unapply(d: Def[_]): Option[(Rep[ShardedTable[R]], Rep[R => (K,V)], Rep[((V,V)) => V]) forSome {type R; type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(map, reduce, _*), _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "MapReduce" =>
          Some((receiver, map, reduce)).asInstanceOf[Option[(Rep[ShardedTable[R]], Rep[R => (K,V)], Rep[((V,V)) => V]) forSome {type R; type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedTable[R]], Rep[R => (K,V)], Rep[((V,V)) => V]) forSome {type R; type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object GroupBy {
      def unapply(d: Def[_]): Option[(Rep[ShardedTable[R]], Rep[R => G]) forSome {type R; type G}] = d match {
        case MethodCall(receiver, method, Seq(by, _*), _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "GroupBy" =>
          Some((receiver, by)).asInstanceOf[Option[(Rep[ShardedTable[R]], Rep[R => G]) forSome {type R; type G}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedTable[R]], Rep[R => G]) forSome {type R; type G}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Join {
      def unapply(d: Def[_]): Option[(Rep[ShardedTable[R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type R; type I; type K}] = d match {
        case MethodCall(receiver, method, Seq(inner, outKey, inKey, _*), _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "Join" =>
          Some((receiver, inner, outKey, inKey)).asInstanceOf[Option[(Rep[ShardedTable[R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type R; type I; type K}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedTable[R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type R; type I; type K}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object gather {
      def unapply(d: Def[_]): Option[Rep[ShardedTable[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "gather" =>
          Some(receiver).asInstanceOf[Option[Rep[ShardedTable[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ShardedTable[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object toArray {
      def unapply(d: Def[_]): Option[Rep[ShardedTable[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ShardedTableElem[_]] && method.getName == "toArray" =>
          Some(receiver).asInstanceOf[Option[Rep[ShardedTable[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ShardedTable[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object ShardedTableCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ShardedTableCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[(Rep[String], Rep[Int], Rep[Int => Table[R]], Rep[R => Int]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(tableName, nShards, createShard, distrib, _*), _) if receiver.elem.isInstanceOf[ShardedTableCompanionElem] && method.getName == "create" =>
          Some((tableName, nShards, createShard, distrib)).asInstanceOf[Option[(Rep[String], Rep[Int], Rep[Int => Table[R]], Rep[R => Int]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[String], Rep[Int], Rep[Int => Table[R]], Rep[R => Int]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkShardedTable[R]
    (tableName: Rep[String], nShards: Rep[Int], distrib: Rep[R => Int], shards: Rep[Array[Table[R]]])(implicit schema: Elem[R], shardKeyPath: String) =
    new ExpShardedTable[R](tableName, nShards, distrib, shards)
  def unmkShardedTable[R:Elem](p: Rep[ShardedTable[R]]) =
    Some((p.tableName, p.nShards, p.distrib, p.shards))

  case class ExpShardedView[R]
      (override val nShards: Rep[Int], override val view: Rep[Int => Table[R]])
      (implicit schema: Elem[R], shardKeyPath: String)
    extends ShardedView[R](nShards, view) with UserTypeDef[BaseTable[R], ShardedView[R]] {
    lazy val selfType = element[ShardedView[R]].asInstanceOf[Elem[BaseTable[R]]]
    override def mirror(t: Transformer) = ExpShardedView[R](t(nShards), t(view))
  }

  lazy val ShardedView: Rep[ShardedViewCompanionAbs] = new ShardedViewCompanionAbs with UserTypeDef[ShardedViewCompanionAbs, ShardedViewCompanionAbs] {
    lazy val selfType = element[ShardedViewCompanionAbs]
    override def mirror(t: Transformer) = this
  }

  object ShardedViewMethods {
    object count {
      def unapply(d: Def[_]): Option[Rep[ShardedView[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "count" =>
          Some(receiver).asInstanceOf[Option[Rep[ShardedView[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ShardedView[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insert {
      def unapply(d: Def[_]): Option[(Rep[ShardedView[R]], Rep[R]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(record, _*), _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "insert" =>
          Some((receiver, record)).asInstanceOf[Option[(Rep[ShardedView[R]], Rep[R]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedView[R]], Rep[R]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insertFrom {
      def unapply(d: Def[_]): Option[(Rep[ShardedView[R]], Arr[R]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(arr, _*), _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "insertFrom" =>
          Some((receiver, arr)).asInstanceOf[Option[(Rep[ShardedView[R]], Arr[R]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedView[R]], Arr[R]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object filter {
      def unapply(d: Def[_]): Option[(Rep[ShardedView[R]], Rep[R => Boolean]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(predicate, _*), _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "filter" =>
          Some((receiver, predicate)).asInstanceOf[Option[(Rep[ShardedView[R]], Rep[R => Boolean]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedView[R]], Rep[R => Boolean]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Sum {
      def unapply(d: Def[_]): Option[(Rep[ShardedView[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, n, _*), _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "Sum" =>
          Some((receiver, agg, n)).asInstanceOf[Option[(Rep[ShardedView[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedView[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Max {
      def unapply(d: Def[_]): Option[(Rep[ShardedView[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, o, _*), _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "Max" =>
          Some((receiver, agg, o)).asInstanceOf[Option[(Rep[ShardedView[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedView[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Min {
      def unapply(d: Def[_]): Option[(Rep[ShardedView[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, o, _*), _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "Min" =>
          Some((receiver, agg, o)).asInstanceOf[Option[(Rep[ShardedView[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedView[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object MapReduce {
      def unapply(d: Def[_]): Option[(Rep[ShardedView[R]], Rep[R => (K,V)], Rep[((V,V)) => V]) forSome {type R; type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(map, reduce, _*), _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "MapReduce" =>
          Some((receiver, map, reduce)).asInstanceOf[Option[(Rep[ShardedView[R]], Rep[R => (K,V)], Rep[((V,V)) => V]) forSome {type R; type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedView[R]], Rep[R => (K,V)], Rep[((V,V)) => V]) forSome {type R; type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object GroupBy {
      def unapply(d: Def[_]): Option[(Rep[ShardedView[R]], Rep[R => G]) forSome {type R; type G}] = d match {
        case MethodCall(receiver, method, Seq(by, _*), _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "GroupBy" =>
          Some((receiver, by)).asInstanceOf[Option[(Rep[ShardedView[R]], Rep[R => G]) forSome {type R; type G}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedView[R]], Rep[R => G]) forSome {type R; type G}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Join {
      def unapply(d: Def[_]): Option[(Rep[ShardedView[R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type R; type I; type K}] = d match {
        case MethodCall(receiver, method, Seq(inner, outKey, inKey, _*), _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "Join" =>
          Some((receiver, inner, outKey, inKey)).asInstanceOf[Option[(Rep[ShardedView[R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type R; type I; type K}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[ShardedView[R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type R; type I; type K}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object gather {
      def unapply(d: Def[_]): Option[Rep[ShardedView[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "gather" =>
          Some(receiver).asInstanceOf[Option[Rep[ShardedView[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ShardedView[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object toArray {
      def unapply(d: Def[_]): Option[Rep[ShardedView[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ShardedViewElem[_]] && method.getName == "toArray" =>
          Some(receiver).asInstanceOf[Option[Rep[ShardedView[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[ShardedView[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object ShardedViewCompanionMethods {
    object defaultOf {
      def unapply(d: Def[_]): Option[Unit forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[ShardedViewCompanionElem] && method.getName == "defaultOf" =>
          Some(()).asInstanceOf[Option[Unit forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Unit forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object create {
      def unapply(d: Def[_]): Option[(Rep[Int], Rep[Int => Table[R]], String) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(nShards, view, shardKeyPath, _*), _) if receiver.elem.isInstanceOf[ShardedViewCompanionElem] && method.getName == "create" =>
          Some((nShards, view, shardKeyPath)).asInstanceOf[Option[(Rep[Int], Rep[Int => Table[R]], String) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Int], Rep[Int => Table[R]], String) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  def mkShardedView[R]
    (nShards: Rep[Int], view: Rep[Int => Table[R]])(implicit schema: Elem[R], shardKeyPath: String) =
    new ExpShardedView[R](nShards, view)
  def unmkShardedView[R:Elem](p: Rep[ShardedView[R]]) =
    Some((p.nShards, p.view))

  object TableMethods {
    object tableName {
      def unapply(d: Def[_]): Option[Rep[Table[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "tableName" =>
          Some(receiver).asInstanceOf[Option[Rep[Table[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Table[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Select {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => P]) forSome {type R; type P}] = d match {
        case MethodCall(receiver, method, Seq(projection, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "Select" =>
          Some((receiver, projection)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => P]) forSome {type R; type P}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => P]) forSome {type R; type P}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object filter {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => Boolean]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(predicate, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "filter" =>
          Some((receiver, predicate)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => Boolean]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => Boolean]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object singleton {
      def unapply(d: Def[_]): Option[Rep[Table[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "singleton" =>
          Some(receiver).asInstanceOf[Option[Rep[Table[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Table[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Sum {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, n, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "Sum" =>
          Some((receiver, agg, n)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Max {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, o, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "Max" =>
          Some((receiver, agg, o)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Min {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, o, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "Min" =>
          Some((receiver, agg, o)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => E], Ordering[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Avg {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}] = d match {
        case MethodCall(receiver, method, Seq(agg, n, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "Avg" =>
          Some((receiver, agg, n)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => E], Numeric[E]) forSome {type R; type E}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object count {
      def unapply(d: Def[_]): Option[Rep[Table[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "count" =>
          Some(receiver).asInstanceOf[Option[Rep[Table[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Table[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object MapReduce {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => (K,V)], Rep[((V,V)) => V]) forSome {type R; type K; type V}] = d match {
        case MethodCall(receiver, method, Seq(map, reduce, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "MapReduce" =>
          Some((receiver, map, reduce)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => (K,V)], Rep[((V,V)) => V]) forSome {type R; type K; type V}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => (K,V)], Rep[((V,V)) => V]) forSome {type R; type K; type V}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object OrderBy {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => O], Ordering[O]) forSome {type R; type O}] = d match {
        case MethodCall(receiver, method, Seq(by, o, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "OrderBy" =>
          Some((receiver, by, o)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => O], Ordering[O]) forSome {type R; type O}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => O], Ordering[O]) forSome {type R; type O}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object GroupBy {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => G]) forSome {type R; type G}] = d match {
        case MethodCall(receiver, method, Seq(by, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "GroupBy" =>
          Some((receiver, by)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => G]) forSome {type R; type G}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => G]) forSome {type R; type G}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Join {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type R; type I; type K}] = d match {
        case MethodCall(receiver, method, Seq(inner, outKey, inKey, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "Join" =>
          Some((receiver, inner, outKey, inKey)).asInstanceOf[Option[(Rep[Table[R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type R; type I; type K}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[Table[I]], Rep[R => K], Rep[I => K]) forSome {type R; type I; type K}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insert {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(record, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "insert" =>
          Some((receiver, record)).asInstanceOf[Option[(Rep[Table[R]], Rep[R]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object insertFrom {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Arr[R]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(arr, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "insertFrom" =>
          Some((receiver, arr)).asInstanceOf[Option[(Rep[Table[R]], Arr[R]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Arr[R]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object Delete {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => Boolean]) forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(predicate, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "Delete" =>
          Some((receiver, predicate)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => Boolean]) forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => Boolean]) forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object drop {
      def unapply(d: Def[_]): Option[Rep[Table[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "drop" =>
          Some(receiver).asInstanceOf[Option[Rep[Table[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Table[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object PrimaryKey {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => K]) forSome {type R; type K}] = d match {
        case MethodCall(receiver, method, Seq(key, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "PrimaryKey" =>
          Some((receiver, key)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => K]) forSome {type R; type K}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => K]) forSome {type R; type K}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object SecondaryKey {
      def unapply(d: Def[_]): Option[(Rep[Table[R]], Rep[R => K]) forSome {type R; type K}] = d match {
        case MethodCall(receiver, method, Seq(key, _*), _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "SecondaryKey" =>
          Some((receiver, key)).asInstanceOf[Option[(Rep[Table[R]], Rep[R => K]) forSome {type R; type K}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[(Rep[Table[R]], Rep[R => K]) forSome {type R; type K}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object gather {
      def unapply(d: Def[_]): Option[Rep[Table[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "gather" =>
          Some(receiver).asInstanceOf[Option[Rep[Table[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Table[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object toArray {
      def unapply(d: Def[_]): Option[Rep[Table[R]] forSome {type R}] = d match {
        case MethodCall(receiver, method, _, _) if receiver.elem.isInstanceOf[TableElem[_, _, _]] && method.getName == "toArray" =>
          Some(receiver).asInstanceOf[Option[Rep[Table[R]] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Table[R]] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }

  object TableCompanionMethods {
    // WARNING: Cannot generate matcher for method `defaultOf`: Method's return type Default[Rep[Table[R]]] is not a Rep

    object create {
      def unapply(d: Def[_]): Option[Rep[String] forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(tableName, _*), _) if receiver.elem.isInstanceOf[TableCompanionElem] && method.getName == "create" =>
          Some(tableName).asInstanceOf[Option[Rep[String] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[String] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }

    object createShard {
      def unapply(d: Def[_]): Option[Rep[Int] forSome {type R}] = d match {
        case MethodCall(receiver, method, Seq(shardNo, _*), _) if receiver.elem.isInstanceOf[TableCompanionElem] && method.getName == "createShard" =>
          Some(shardNo).asInstanceOf[Option[Rep[Int] forSome {type R}]]
        case _ => None
      }
      def unapply(exp: Exp[_]): Option[Rep[Int] forSome {type R}] = exp match {
        case Def(d) => unapply(d)
        case _ => None
      }
    }
  }
}
