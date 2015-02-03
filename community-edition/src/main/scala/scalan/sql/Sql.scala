package scalan.sql

import scalan._
import scalan.collections._
import scalan.common.Default


trait Sql extends Base { sql: SqlDsl =>

  def uniqueIndexSearch[K:Elem,R:Elem](index: Rep[UniqueIndex[K,R]], predicate: Rep[R => Boolean]): Rep[Table[R]]
  def nonUniqueIndexSearch[K:Elem,R:Elem](index: Rep[NonUniqueIndex[K,R]], predicate: Rep[R => Boolean]): Rep[Table[R]]
  def joinTables[O:Elem, I:Elem, K:Elem](outer: Rep[Table[O]], inner: Rep[Table[I]], outKey: Rep[O => K], inKey: Rep[I => K]): Rep[Table[(O,I)]]
  def joinShardedTable[O:Elem, I:Elem, K:Elem](outer: ShardedTable[O], inner: Rep[Table[I]], outKey: Rep[O => K], inKey: Rep[I => K]): Rep[Table[(O,I)]]
  def joinShardedView[O:Elem, I:Elem, K:Elem](outer: ShardedView[O], inner: Rep[Table[I]], outKey: Rep[O => K], inKey: Rep[I => K]): Rep[Table[(O,I)]]
  def getKeyPath[K](key:Rep[K]): String


  trait Table[R] extends Reifiable[Table[R]] { 
    implicit def schema:Elem[R]
    def tableName:Rep[String]
    def Select[P:Elem](projection: Rep[R=>P]): Rep[Table[P]] = ReadOnlyTable(toArray.mapBy(projection))
    def filter(predicate: Rep[R => Boolean]): Rep[Table[R]] = ReadOnlyTable(toArray.filterBy(predicate))

    def singleton: Rep[R] = toArray.apply(0)

    def Sum[E:Elem](agg: Rep[R=>E])(implicit n:Numeric[E]): Rep[E] = toArray.sumBy(agg)
      //toArray.fold[E](n.zero, fun { p => p._1 + agg(p._2) } )
      // toArray.map(agg).sum
    def Max[E:Elem](agg: Rep[R=>E])(implicit o:Ordering[E]): Rep[E] = toArray.mapBy(agg).max
    def Min[E:Elem](agg: Rep[R=>E])(implicit o:Ordering[E]): Rep[E] = toArray.mapBy(agg).min
    def Avg[E:Elem](agg: Rep[R=>E])(implicit n:Numeric[E]): Rep[Double] = toArray.mapBy(agg).avg
    def count: Rep[Int] = toArray.length

    def MapReduce[K: Elem, V: Elem](map: Rep[R => (K, V)], reduce: Rep[((V, V)) => V]): Rep[PMap[K,V]] = toArray.mapReduceBy[K,V](map, reduce)

    def OrderBy[O: Elem](by: Rep[R=>O])(implicit o:Ordering[O]): Arr[R] = toArray.sortBy(by)
    def GroupBy[G: Elem](by: Rep[R=>G]): Rep[MultiMap[G, R]] = MultiMap.fromMap[G, R](toArray.groupBy(by))

    def Join[I:Elem, K:Elem](inner: Rep[Table[I]])(outKey: Rep[R=>K], inKey: Rep[I=>K]): Rep[Table[(R,I)]] = {
      joinTables[R,I,K](self, inner, outKey, inKey)
    } 
         
    def insert(record: Rep[R]): Rep[Table[R]]
    def insertFrom(arr: Arr[R]): Rep[Table[R]] = {
      implicit val t = selfType
      arr.fold[Table[R]](self, fun { p => insert(p._2) })
      //(arr.fold[Int](1, fun { p => (insert(p._2) | p._1) }) | self)
      //arr.fold[Table[R]](self, fun { p => p._1.insert(p._2) })
      //(loopUntil(0)(fun { i => i >= arr.length }, fun { i => (insert(arr(i)) | i + 1) } ) | self)
    }

    // Can not implement this methods properly now because there is no way to delete item from Map in LMS
    def Delete(predicate: Rep[R=>Boolean]): Rep[Table[R]] = ???
    def drop: Rep[Table[R]] = ???

    def PrimaryKey[K:Elem](key: Rep[R=>K]): Rep[Table[R]]
    def SecondaryKey[K:Elem](key: Rep[R=>K]): Rep[Table[R]]

    def gather: Rep[Table[R]] = self

    def toArray: Arr[R]
  }


  implicit def defaultTableElement[R:Elem]: Elem[Table[R]] = element[BaseTable[R]].asElem[Table[R]]

  trait TableCompanion extends TypeFamily1[Table] {
    def defaultOf[R:Elem]: Default[Rep[Table[R]]] = ReadWriteTable.defaultOf[R]
    def create[R:Elem](tableName: Rep[String]): Rep[Table[R]] = ReadWriteTable.create[R](tableName)
    def createShard[R:Elem](shardNo: Rep[Int]): Rep[Table[R]] = ReadWriteTable.createShard[R](shardNo)
  }

  abstract class BaseTable[R](val tableName: Rep[String])(implicit val schema:Elem[R]) extends Table[R] {
    /*
    override def equals(other: Any) = {
      other match {
        case that: BaseTable[_] =>
          (this.schema equals that.schema) && (this.tableName equals that.tableName)
        case _ => false
      }
    }
    */
    def PrimaryKey[K:Elem](key: Rep[R=>K]): Rep[Table[R]] = {
      implicit val keyPath = getKeyPath(key)
      UniqueIndex.create[K,R](tableName + ".pk", self, key)
    }

    def SecondaryKey[K:Elem](key: Rep[R=>K]): Rep[Table[R]] = {
      implicit val keyPath = getKeyPath(key)
      NonUniqueIndex.create[K,R](tableName + ".sk", self, key)
    }

    def toArray: Arr[R] = element[Array[R]].defaultRepValue
    def insert(record: Rep[R]): Rep[Table[R]] = self
  }

  trait BaseTableCompanion extends ConcreteClass1[BaseTable] with TableCompanion {
    override def defaultOf[R:Elem] = Default.defaultVal(BaseTable(""))
  }

  abstract class UniqueIndex[K,R](tableName: Rep[String], val table:Rep[Table[R]], val map:Rep[PMap[K,R]], val getKey: Rep[R=>K])
    (implicit schema:Elem[R], val index:Elem[K], val keyPath: String)
    extends BaseTable[R](tableName)
  {
    override def filter(predicate: Rep[R => Boolean]): Rep[Table[R]] = uniqueIndexSearch(self.asRep[UniqueIndex[K,R]], predicate)

    override def count: Rep[Int] = table.count

    override def insert(record: Rep[R]): Rep[Table[R]] = {
      val key = getKey(record) 
//      ((IF (map.contains(key)) THEN THROW("Unique constraint violation") ELSE map.update(key, record)) | table.insert(record) | self)
      (map.applyIf(key, x => THROW("Unique constraint violation"), () => map.update(key, record)) | table.insert(record) | self)
    }

    override def Join[I:Elem, K:Elem](inner: Rep[Table[I]])(outKey: Rep[R=>K], inKey: Rep[I=>K]): Rep[Table[(R,I)]] = table.Join(inner)(outKey, inKey)

    override def toArray: Arr[R] = table.toArray
  }


  trait UniqueIndexCompanion extends ConcreteClass2[UniqueIndex] with TableCompanion {
    def defaultOf[K:Elem,R:Elem] = {
      implicit val keyPath = ""
      Default.defaultVal(UniqueIndex("", super.defaultOf[R].value, element[PMap[K, R]].defaultRepValue, element[R=>K].defaultRepValue))
    }
    def create[K:Elem,R:Elem](tableName: Rep[String], table: Rep[Table[R]], key: Rep[R=>K]) = {
      implicit val keyPath = getKeyPath(key)
      UniqueIndex(tableName, table, PMap.make[K,R](tableName), key)
    }
  }


  abstract class NonUniqueIndex[K,R](tableName: Rep[String], val table:Rep[Table[R]], val map:Rep[MultiMap[K,R]], val getKey: Rep[R=>K])
    (implicit schema:Elem[R], val index:Elem[K], val keyPath: String)
    extends BaseTable[R](tableName)
  {
    override def filter(predicate: Rep[R => Boolean]): Rep[Table[R]] = nonUniqueIndexSearch(self.asRep[NonUniqueIndex[K,R]], predicate)

    override def count: Rep[Int] = table.count

    override def insert(record: Rep[R]): Rep[Table[R]] = {
      (map.add(getKey(record), record) | table.insert(record) | self)
    }

    override def Join[I:Elem, K:Elem](inner: Rep[Table[I]])(outKey: Rep[R=>K], inKey: Rep[I=>K]): Rep[Table[(R,I)]] = table.Join(inner)(outKey, inKey)

    override def toArray: Arr[R] = table.toArray
  }

  trait NonUniqueIndexCompanion extends ConcreteClass2[NonUniqueIndex] with TableCompanion {
    def defaultOf[K:Elem,R:Elem] = {
      implicit val keyPath = ""
      Default.defaultVal(NonUniqueIndex("", super.defaultOf[R].value, MultiMap.defaultOf[K,R].value, element[R=>K].defaultRepValue))
    }
    def create[K:Elem,R:Elem](tableName: Rep[String], table: Rep[Table[R]], key: Rep[R=>K]) = {
      implicit val keyPath = getKeyPath(key)
      NonUniqueIndex(tableName, table, MultiMap.make[K, R](tableName), key)
    }
  }

  abstract class ReadWriteTable[R](tableName: Rep[String], val records: Rep[ArrayBuffer[R]])
                                 (implicit schema:Elem[R])
    extends BaseTable[R](tableName)
  {
    override def count: Rep[Int] = records.length

    override def insert(record: Rep[R]): Rep[Table[R]] = {
      ((records += record) | self)
    }

    override def toArray = records.toArray
  }

  trait ReadWriteTableCompanion extends ConcreteClass1[ReadWriteTable] with TableCompanion {
    override def defaultOf[R:Elem] = Default.defaultVal(ReadWriteTable("" , element[ArrayBuffer[R]].defaultRepValue))
    override def create[R:Elem](tableName: Rep[String]): Rep[Table[R]] = ReadWriteTable(tableName, ArrayBuffer.make[R](tableName))
    override def createShard[R:Elem](shardNo: Rep[Int]): Rep[Table[R]] = {
      val tableName = shardNo.toStr
      ReadWriteTable(tableName, ArrayBuffer.make[R](tableName))
    }
  }
  
  abstract class ReadOnlyTable[R](val records: Rep[Array[R]])
                                  (implicit schema:Elem[R])
    extends BaseTable[R]("tmptab")
  {
    override def count: Rep[Int] = records.length

    override def insert(record: Rep[R]): Rep[Table[R]] = !!!

    override def PrimaryKey[K:Elem](key: Rep[R=>K]): Rep[Table[R]] = {
      implicit val keyPath = getKeyPath(key)
      UniqueIndex[K,R](tableName + ".pk", self, PMap.create[K,R](records.length, fun { i => (key(records(i)), records(i))}), key)
    }

    override def SecondaryKey[K:Elem](key: Rep[R=>K]): Rep[Table[R]] = {
      implicit val keyPath = getKeyPath(key)
      NonUniqueIndex[K,R](tableName + ".sk", self, MultiMap.fromArray[K,R](Array.tabulate[(K,R)](records.length)(i => (key(records(i)), records(i)))), key)
    }

    override def toArray = records
  }

  trait ReadOnlyTableCompanion extends ConcreteClass1[ReadOnlyTable] with TableCompanion {
    override def defaultOf[R:Elem] = Default.defaultVal(ReadOnlyTable(element[Array[R]].defaultRepValue))
  }


  abstract class PairTable[R1,R2](val left:Rep[Table[R1]], val right:Rep[Table[R2]])
                                 (implicit val leftSchema:Elem[R1], rightSchema:Elem[R2])
    extends BaseTable[(R1,R2)](left.tableName + "+" + right.tableName)
  {
    override def count: Rep[Int] = left.count
    override def insert(record: Rep[(R1,R2)]): Rep[Table[(R1,R2)]] =  {
      left.insert(record._1) |
      right.insert(record._2) |
      self
    }

    override def insertFrom(arr: Arr[(R1,R2)]): Rep[Table[(R1,R2)]] = {
      PairTable[R1,R2](left.insertFrom(arr.map(_._1)), right.insertFrom(arr.map(_._2)))
    }


    override def toArray = left.toArray zip right.toArray
  }

  trait PairTableCompanion extends ConcreteClass2[PairTable] with TableCompanion {
    override def defaultOf[R1,R2](implicit leftSchema:Elem[R1], rightSchema:Elem[R2]) = {
      implicit val schema:PairElem[R1,R2] = PairElem(leftSchema,rightSchema)
      Default.defaultVal(PairTable(Table.defaultOf[R1].value, Table.defaultOf[R2].value))
    }
    def create[R1,R2](l:Rep[Table[R1]], r:Rep[Table[R2]])(implicit leftSchema:Elem[R1], rightSchema:Elem[R2]) = {
      implicit val schema:PairElem[R1,R2] = PairElem(leftSchema, rightSchema)
      PairTable[R1,R2]( l, r)
    }
 }

  abstract class ShardedTable[R](tableName: Rep[String], val nShards: Rep[Int], val distrib: Rep[R=>Int], val shards: Rep[Array[Table[R]]])
                                (implicit schema:Elem[R], val shardKeyPath: String)
    extends BaseTable[R](tableName)
  {
    override def count: Rep[Int] = shards(0).count

    override def insert(record: Rep[R]): Rep[Table[R]] = {
      val node = distrib(record) % nShards
      shards(node).insert(record)
    }

    override def insertFrom(arr: Arr[R]): Rep[Table[R]] = {
      //ShardedTable.create[R](nShards, (node: Rep[Int]) => shards(node).insertFrom(arr.stride(node, (arr.length + nShards - node - 1) /! nShards, nShards)), distrib)
      ShardedTable(tableName, nShards, distrib, par(nShards, (node: Rep[Int]) => shards(node).insertFrom(arr.filter(r => distrib(r) % nShards === node))))
    }

    override def filter(predicate: Rep[R => Boolean]): Rep[Table[R]] = {
      ShardedView.create(nShards, fun { node => shards(node).filter(predicate) })
    }

    override def Sum[E: Elem](agg: Rep[R => E])(implicit n: Numeric[E]): Rep[E] = {
      par(nShards, (node: Rep[Int]) => shards(node).Sum(agg)).sum
    }

    // TODO: correctly handle empty shard
    override def Max[E: Elem](agg: Rep[R => E])(implicit o: Ordering[E]): Rep[E] = {
      par(nShards, (node: Rep[Int]) => shards(node).Max(agg)).max
    }

    // TODO: correctly handle empty shard
    override def Min[E: Elem](agg: Rep[R => E])(implicit o: Ordering[E]): Rep[E] = {
      par(nShards, (node: Rep[Int]) => shards(node).Min(agg)).min
    }


    override def MapReduce[K: Elem, V: Elem](map: Rep[R => (K, V)], reduce: Rep[((V, V)) => V]): Rep[PMap[K,V]] = {
      par(nShards, (node: Rep[Int]) => shards(node).MapReduce[K,V](map, reduce)).fold[PMap[K,V]](PMap.empty[K,V], fun { p => p._1.reduce(p._2, reduce) })
    }

    override def GroupBy[G: Elem](by: Rep[R=>G]): Rep[MultiMap[G, R]] = {
      par(nShards, (node: Rep[Int]) => shards(node).GroupBy[G](by)).fold[MultiMap[G, R]](MultiMap.empty[G, R], fun { p => p._1.union(p._2) })
    }

    override def Join[I:Elem, K:Elem](inner: Rep[Table[I]])(outKey: Rep[R=>K], inKey: Rep[I=>K]): Rep[Table[(R,I)]] = {
      joinShardedTable[R,I,K](this, inner, outKey, inKey)
    }

    override def gather: Rep[Table[R]] = ReadOnlyTable(toArray)

    override def toArray = shards.fold[ArrayBuffer[R]](ArrayBuffer.empty[R], fun { p => p._1 ++= p._2.toArray}).toArray
  }

  trait ShardedTableCompanion extends ConcreteClass1[ShardedTable] with TableCompanion {
    override def defaultOf[R:Elem] = {
      implicit val shardKeyPath = ""
      Default.defaultVal(ShardedTable("", 0,element[R => Int].defaultRepValue, element[Array[Table[R]]].defaultRepValue))
    }
    def create[R:Elem](tableName: Rep[String], nShards: Rep[Int], createShard:Rep[Int=>Table[R]], distrib: Rep[R=>Int]) = {
      implicit val shardKeyPath = getKeyPath(distrib)
      ShardedTable(tableName, nShards, distrib, Array.repeat[Table[R]](nShards)(createShard))
    }
  }

  abstract class ShardedView[R](val nShards: Rep[Int], val view: Rep[Int => Table[R]])
                                (implicit schema:Elem[R], val shardKeyPath:String)
    extends BaseTable[R]("view")
  {
    override def count: Rep[Int] = {
      par(nShards, (node: Rep[Int]) => view(node).count).sum
    }

    override def insert(record: Rep[R]): Rep[Table[R]] = !!!

    override def insertFrom(arr: Arr[R]): Rep[Table[R]] = !!!

    override def filter(predicate: Rep[R => Boolean]): Rep[Table[R]] = {
      ShardedView.create(nShards, fun { node => view(node).filter(predicate) })
    }

    override def Sum[E: Elem](agg: Rep[R => E])(implicit n: Numeric[E]): Rep[E] = {
      par(nShards, (node: Rep[Int]) => view(node).Sum(agg)).sum
    }

    // TODO: correctly handle empty shard
    override def Max[E: Elem](agg: Rep[R => E])(implicit o: Ordering[E]): Rep[E] = {
      par(nShards, (node: Rep[Int]) => view(node).Max(agg)).max
    }

    // TODO: correctly handle empty shard
    override def Min[E: Elem](agg: Rep[R => E])(implicit o: Ordering[E]): Rep[E] = {
      par(nShards, (node: Rep[Int]) => view(node).Min(agg)).min
    }

    override def MapReduce[K: Elem, V: Elem](map: Rep[R => (K, V)], reduce: Rep[((V, V)) => V]): Rep[PMap[K,V]] = {
      par(nShards, (node: Rep[Int]) => view(node).MapReduce[K,V](map, reduce)).fold[PMap[K,V]](PMap.empty[K,V], fun { p => p._1.reduce(p._2, reduce) })
    }

    override def GroupBy[G: Elem](by: Rep[R=>G]): Rep[MultiMap[G, R]] = {
      par(nShards, (node: Rep[Int]) => view(node).GroupBy[G](by)).fold[MultiMap[G, R]](MultiMap.empty[G, R], fun { p => p._1.union(p._2) })
    }

    override def Join[I:Elem, K:Elem](inner: Rep[Table[I]])(outKey: Rep[R=>K], inKey: Rep[I=>K]): Rep[Table[(R,I)]] = {
      joinShardedView[R,I,K](this, inner, outKey, inKey)
    }

    override def gather: Rep[Table[R]] = ReadOnlyTable(toArray)

    override def toArray = {
      val results = par(nShards, (node: Rep[Int]) => view(node).toArray)
      results.fold[ArrayBuffer[R]](ArrayBuffer.empty[R], fun { p => p._1 ++= p._2}).toArray
    }
  }

  trait ShardedViewCompanion extends ConcreteClass1[ShardedView] with TableCompanion {
    override def defaultOf[R:Elem] = {
      implicit val shardKeyPath = ""
      Default.defaultVal(ShardedView(element[Int].defaultRepValue, element[Int => Table[R]].defaultRepValue))
    }
    def create[R:Elem](nShards: Rep[Int], view: Rep[Int=>Table[R]])(implicit shardKeyPath:String) = ShardedView(nShards, view)
  }
}

trait SqlDsl extends ScalanDsl with impl.SqlAbs with Sql with MultiMapsDsl {

  implicit class TableExt[R](table: Rep[Table[R]])(implicit schema: Elem[R]) {
    def select[P: Elem](projection: Rep[R] => Rep[P]): Rep[Table[P]] = table.Select(fun(projection))

    def where(predicate: Rep[R] => Rep[Boolean]): Rep[Table[R]] = table.filter(fun(predicate))

    def sum[E: Elem](agg: Rep[R] => Rep[E])(implicit n: Numeric[E]): Rep[E] = table.Sum(fun(agg))

    def max[E: Elem](agg: Rep[R] => Rep[E])(implicit o: Ordering[E]): Rep[E] = table.Max(fun(agg))

    def min[E: Elem](agg: Rep[R] => Rep[E])(implicit o: Ordering[E]): Rep[E] = table.Min(fun(agg))

    def avg[E: Elem](agg: Rep[R] => Rep[E])(implicit n: Numeric[E]): Rep[Double] = table.Avg(fun(agg))

    def orderBy[O: Elem](by: Rep[R] => Rep[O])(implicit o: Ordering[O]): Arr[R] = table.OrderBy(fun(by))

    def groupBy[G: Elem](by: Rep[R] => Rep[G]): Rep[MultiMap[G, R]] = table.GroupBy(by)

    def join[I: Elem, K: Elem](inner: Rep[Table[I]])(outKey: Rep[R] => Rep[K], inKey: Rep[I] => Rep[K]): Rep[Table[(R, I)]] = table.Join(inner)(outKey, inKey)

    def delete(predicate: Rep[R] => Rep[Boolean]): Rep[Table[R]] = table.Delete(fun(predicate))

    def primaryKey[K: Elem](key: Rep[R] => Rep[K]): Rep[Table[R]] = table.PrimaryKey(fun(key))

    def secondaryKey[K: Elem](key: Rep[R] => Rep[K]): Rep[Table[R]] = table.SecondaryKey(fun(key))

    def mapReduce[K: Elem, V: Elem](map: Rep[R] => Rep[(K, V)], reduce: (Rep[V], Rep[V]) => Rep[V]): Rep[PMap[K,V]] = table.MapReduce[K,V](fun(map), fun2(reduce))
  }

}

trait SqlDslSeq extends SqlDsl with impl.SqlSeq with ScalanCtxSeq with MultiMapsDslSeq {
  def uniqueIndexSearch[K:Elem,R:Elem](index: Rep[UniqueIndex[K,R]], predicate: Rep[R=>Boolean]): Rep[Table[R]] = {
    ReadOnlyTable(index.table.toArray.filterBy(predicate))
  }

  def nonUniqueIndexSearch[K:Elem,R:Elem](index: Rep[NonUniqueIndex[K,R]], predicate: Rep[R=>Boolean]): Rep[Table[R]] = {
    ReadOnlyTable(index.table.toArray.filterBy(predicate))
  }

  // Right now supports only join on primary keys (unique index should exist)       
  def joinTables[O:Elem, I:Elem, K:Elem](outer: Rep[Table[O]], inner: Rep[Table[I]], outKey: Rep[O=>K], inKey: Rep[I=>K]): Rep[Table[(O,I)]] = {
    val map = MultiMap.fromArray[K,I](genericArrayOps(inner.toArray).map(i => (inKey(i), i)))
    ReadOnlyTable[(O,I)](genericArrayOps(outer.toArray).flatMap(o => genericArrayOps(map(outKey(o)).toArray).map(i => (o, i))))
  }
  def joinShardedTable[O:Elem, I:Elem, K:Elem](outer: ShardedTable[O], inner: Rep[Table[I]], outKey: Rep[O=>K], inKey: Rep[I=>K]): Rep[Table[(O,I)]] = {
    joinTables[O,I,K](outer.self, inner, outKey, inKey)
  }
  def joinShardedView[O:Elem, I:Elem, K:Elem](outer: ShardedView[O], inner: Rep[Table[I]], outKey: Rep[O=>K], inKey: Rep[I=>K]): Rep[Table[(O,I)]] = {
    joinTables[O,I,K](outer.self, inner, outKey, inKey)
  }
  def getKeyPath[K](key:Rep[K]): String = ""
}

trait SqlDslExp extends SqlDsl with impl.SqlExp with ScalanExp with MultiMapsDslExp {
  def getKeyPath[K](key: Rep[K]): String = {
    key match {
      case Def(l: Lambda[_, _]) => getKeyPath(l.y)
      case Def(Tup(l, r)) => getKeyPath(l) + "+" + getKeyPath(r)
      case Def(First(t)) => getKeyPath(t) + "1"
      case Def(Second(t)) => getKeyPath(t) + "2"
      case Def(_) => "?"
      case _ => "_"
    }
  }

  def matchPath(sym: Exp[_], exp: Exp[_], path: String, i: Int): Int = {
    exp match {
      case Def(Tup(l, r)) => {
        val p = matchPath(sym, l, path, i)
        if (p >= 0 && p < path.length && path(p) == '+') matchPath(sym, r, path, p + 1)
        else -1
      }
      case Def(First(t)) => {
        val p = matchPath(sym, t, path, i)
        if (p >= 0 && p < path.length && path(p) == '1') p + 1
        else -1
      }
      case Def(Second(t)) => {
        val p = matchPath(sym, t, path, i)
        if (p >= 0 && p < path.length && path(p) == '2') p + 1
        else -1
      }
      case Def(_) => -1
      case _ => if (i < path.length && path(i) == '_' && exp.equals(sym)) i + 1 else -1
    }
  }

  def isSameColumn(sym: Exp[_], exp: Exp[_], path: String) = matchPath(sym, exp, path, 0) == path.length

  def isKeyValue(sym: Exp[_], exp: Exp[_]): Boolean = {
    exp match {
      case Def(Const(_)) => true
      case Def(Tup(l, r)) => isKeyValue(sym, l) && isKeyValue(sym, r)
      case Def(First(t)) => isKeyValue(sym, t)
      case Def(Second(t)) => isKeyValue(sym, t)
      case Def(ApplyBinOp(op, l, r)) => isKeyValue(sym, l) && isKeyValue(sym, r)
      case Def(ApplyUnOp(op, opd)) => isKeyValue(sym, opd)
      case Def(_) => false
      case _ => !exp.equals(sym)
    }
  }

  def containsKey(rec: Exp[_], predicate: Exp[_], key: String): Boolean = {
    predicate match {
      case Def(ApplyBinOp(op, lhs, rhs)) => {
        op.asInstanceOf[BinOp[_, _]] match {
          case And => containsKey(rec, lhs, key) ^ containsKey(rec, rhs, key)
          case Equals() => (isSameColumn(rec, lhs, key) && isKeyValue(rec, rhs)) || (isSameColumn(rec, rhs, key) && isKeyValue(rec, lhs))
          case _ => false
        }
      }
      case _ => false
    }
  }

  def getLambdaDef(exp: Exp[_]): Lambda[_, _] = {
    exp match {
      case Def(l: Lambda[_, _]) => l
    }
  }

  def excludeConjunct(rec: Exp[_], predicate: Exp[_], key: String): Option[Exp[_]] = {
    predicate match {
      case Def(ApplyBinOp(op, lhs, rhs)) => {
        op.asInstanceOf[BinOp[_, _]] match {
          case And => {
            (excludeConjunct(rec, lhs, key), excludeConjunct(rec, rhs, key)) match {
              case (Some(c), None) => Some(c)
              case (None, Some(c)) => Some(c)
              case (Some(c1), Some(c2)) => Some(predicate)
              case _ => None
            }
          }
          case Equals() => {
            if ((isSameColumn(rec, lhs, key) && isKeyValue(rec, rhs)) || (isSameColumn(rec, rhs, key) && isKeyValue(rec, lhs))) None
            else Some(predicate)
          }
          case _ => Some(predicate)
        }
      }
      case _ => Some(predicate)
    }
  }

  def excludeKey(predicate: Exp[_], key: String): Option[Exp[_]] = {
    predicate match {
      case Def(l1: Lambda[a, b]) => {
        excludeConjunct(l1.x, l1.y, key) match {
          case Some(condition) => {
            implicit val eA = l1.eA
            val newSym = fresh[a => Boolean]
            val newLam = new Lambda[a, Boolean](None, l1.x, condition.asInstanceOf[Exp[Boolean]], newSym, l1.mayInline)
            Some(toExp(newLam, newSym))
          }
          case _ => None
        }
      }
    }
  }

  def extractConjunct(rec: Exp[_], predicate: Exp[_], key: String): Option[Exp[_]] = {
    predicate match {
      case Def(ApplyBinOp(op, lhs, rhs)) => {
        op.asInstanceOf[BinOp[_, _]] match {
          case And => {
            (extractConjunct(rec, lhs, key), extractConjunct(rec, rhs, key)) match {
              case (Some(c), None) => Some(c)
              case (None, Some(c)) => Some(c)
              case _ => None
            }
          }
          case Equals() => {
            if (isSameColumn(rec, lhs, key) && isKeyValue(rec, rhs)) Some(rhs)
            else if (isSameColumn(rec, rhs, key) && isKeyValue(rec, lhs)) Some(lhs)
            else None
          }
          case _ => None
        }
      }
      case _ => None
    }
  }

  def extractKeyValue(lambda: Lambda[_, _], key: String): Exp[_] = {
    extractConjunct(lambda.x, lambda.y, key).get
  }

  def uniqueIndexSearch[K: Elem, R: Elem](index: Rep[UniqueIndex[K, R]], predicate: Rep[R => Boolean]): Rep[Table[R]] = {
    val keyPath = index.keyPath
    val predicateDef = getLambdaDef(predicate)
    if (containsKey(predicateDef.x, predicateDef.y, keyPath)) {
      val key = extractKeyValue(predicateDef, keyPath).asInstanceOf[Exp[K]]
      excludeKey(predicate, keyPath) match {
        case Some(refinement) =>
          //          ReadOnlyTable(IF (index.map.contains(key)) THEN ArrayBuffer(index.map.apply(key)).toArray.filterBy(refinement.asInstanceOf[Exp[R=>Boolean]]) ELSE ArrayBuffer.empty[R].toArray)
          ReadOnlyTable(index.map.applyIf(key, ArrayBuffer(_).toArray.filterBy(refinement.asInstanceOf[Exp[R => Boolean]]), () => ArrayBuffer.empty[R].toArray))
        case None =>
          //          ReadOnlyTable(IF (index.map.contains(key)) THEN ArrayBuffer(index.map.apply(key)).toArray ELSE ArrayBuffer.empty[R].toArray)
          ReadOnlyTable(index.map.applyIf(key, ArrayBuffer(_).toArray, () => ArrayBuffer.empty[R].toArray))
      }
    } else {
      //      println("Failed to find path '" + keyPath + "' in unique index")
      index.table.filter(predicate)
    }
  }

  def nonUniqueIndexSearch[K: Elem, R: Elem](index: Rep[NonUniqueIndex[K, R]], predicate: Rep[R => Boolean]): Rep[Table[R]] = {
    val keyPath = index.keyPath
    val predicateDef = getLambdaDef(predicate)
    if (containsKey(predicateDef.x, predicateDef.y, keyPath)) {
      val key = extractKeyValue(predicateDef, keyPath).asInstanceOf[Exp[K]]
      excludeKey(predicate, keyPath) match {
        case Some(refinement) =>
          //          ReadOnlyTable(IF (index.map.contains(key)) THEN index.map.apply(key).toArray.filterBy(refinement.asInstanceOf[Exp[R=>Boolean]]) ELSE ArrayBuffer.empty[R].toArray)
          ReadOnlyTable(index.map.applyIf[Array[R]](key, _.toArray.filterBy(refinement.asInstanceOf[Exp[R => Boolean]]), () => ArrayBuffer.empty[R].toArray))
        case None =>
          //          ReadOnlyTable(IF (index.map.contains(key)) THEN index.map.apply(key).toArray ELSE ArrayBuffer.empty[R].toArray)
          ReadOnlyTable(index.map.applyIf[Array[R]](key, _.toArray, () => ArrayBuffer.empty[R].toArray))
      }
    } else {
      //     println("Failed to find path '" + keyPath + "' in non-unique index")
      index.table.filter(predicate)
    }
  }

  def joinTables[O: Elem, I: Elem, K: Elem](outer: Rep[Table[O]], inner: Rep[Table[I]], outKey: Rep[O => K], inKey: Rep[I => K]): Rep[Table[(O, I)]] = {
    inner match {
      case Def(i: UniqueIndex[_, _]) => {
        val index = i.asInstanceOf[UniqueIndex[K, I]]
        val keyDef = getLambdaDef(inKey)
        if (isSameColumn(keyDef.x, keyDef.y, index.keyPath)) {
          ReadOnlyTable[(O, I)](outer.toArray.map(x => (x, index.map(outKey(x)))))
        } else {
          joinTables[O, I, K](outer, index.table, outKey, inKey)
        }
      }
      case Def(i: NonUniqueIndex[_, _]) => {
        val index = i.asInstanceOf[NonUniqueIndex[K, I]]
        val keyDef = getLambdaDef(inKey)
        if (isSameColumn(keyDef.x, keyDef.y, index.keyPath)) {
          ReadOnlyTable[(O, I)](outer.toArray.flatMap(o => index.map(outKey(o)).toArray.map(i => (o, i))))
        } else {
          joinTables[O, I, K](outer, index.table, outKey, inKey)
        }
      }
      case _ => {
        val map = MultiMap.fromArray[K,I](inner.toArray.map(i => (inKey(i), i)))
        ReadOnlyTable[(O, I)](outer.toArray.flatMap(o => map(outKey(o)).toArray.map(i => (o, i))))
      }
    }
  }

  def getShardKeyPath(exp: Exp[_]): String = {
    exp match {
      case Def(m: MethodCall[_]) =>
        if (m.method.getDeclaringClass == classOf[ShardedTableCompanion] && m.method.getName == "create") {
          getKeyPath(m.args(2).asInstanceOf[Exp[_]])
        } else {
          getShardKeyPath(m.receiver)
        }
      case Def(t: ExpShardedTable[_]) => t.shardKeyPath
      case Def(t: ExpShardedView[_]) => t.shardKeyPath
      case _ => ""
    }
  }

  def joinShardedTable[O: Elem, I: Elem, K: Elem](outer: ShardedTable[O], inner: Rep[Table[I]], outKey: Rep[O => K], inKey: Rep[I => K]): Rep[Table[(O, I)]] = {
    val inKeyDef = getLambdaDef(inKey)
    val outKeyDef = getLambdaDef(outKey)
    val outerShardKeyPath = outer.shardKeyPath
    val innerShardKeyPath = getShardKeyPath(inner)
    implicit val shardKeyPath = outerShardKeyPath + "1"
    if (isSameColumn(outKeyDef.x, outKeyDef.y, outerShardKeyPath) && isSameColumn(inKeyDef.x, inKeyDef.y, innerShardKeyPath)) {
      ShardedView.create[(O, I)](outer.nShards, (node: Rep[Int]) => outer.self.asRep[ShardedTable[O]].shards(node).Join(inner.asRep[ShardedTable[I]].shards(node))(outKey, inKey))
    } else {
      ShardedView.create[(O, I)](outer.nShards, (node: Rep[Int]) => outer.self.asRep[ShardedTable[O]].shards(node).Join(inner.gather)(outKey, inKey))
    }
  }

  def joinShardedView[O: Elem, I: Elem, K: Elem](outer: ShardedView[O], inner: Rep[Table[I]], outKey: Rep[O => K], inKey: Rep[I => K]): Rep[Table[(O, I)]] = {
    val inKeyDef = getLambdaDef(inKey)
    val outKeyDef = getLambdaDef(outKey)
    val outerShardKeyPath = outer.shardKeyPath
    val innerShardKeyPath = getShardKeyPath(inner)
    implicit val shardKeyPath = outerShardKeyPath + "1"
    if (isSameColumn(outKeyDef.x, outKeyDef.y, outerShardKeyPath) && isSameColumn(inKeyDef.x, inKeyDef.y, innerShardKeyPath)) {
      ShardedView.create[(O, I)](outer.nShards, (node: Rep[Int]) => outer.self.asRep[ShardedView[O]].view(node).Join(inner.asRep[ShardedTable[I]].shards(node))(outKey, inKey))
    } else {
      ShardedView.create[(O, I)](outer.nShards, (node: Rep[Int]) => outer.self.asRep[ShardedView[O]].view(node).Join(inner.gather)(outKey, inKey))
    }
  }
}
