/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.fpm

import oscar.algo.reversible._
import oscar.cp._
import oscar.cp.CPIntVar
import oscar.cp.core._
import oscar.cp.core.CPOutcome._

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.fpm.PrefixSpan.Postfix

// Array implementation

/**
 * Calculate all patterns of a projected database in local mode.
 *
 * @param prefix the current prefix
 * @param minSup minimal support for a frequent pattern
 * @param minPatternLength min pattern length for a frequent pattern
 * @param maxPatternLength max pattern length for a frequent pattern
 * @param outPutItemSetPermutation A boolean value that define whether running the algorithm
 *                                 on <(1,2) 3> should output (1,2) 3; 1 3; 2 3 or just the
 *                                 last two ...
 */
private[fpm] class PPICSpark( val prefix: Array[Int],
                              val minSup: Long,
                              val minPatternLength: Int,
                              val maxPatternLength: Int,
                              val outPutItemSetPermutation: Boolean)
  extends CPModel with App with Logging with Serializable{

  // Define separator
  var separator = 0
  var epsilon = -1

  var lenSeqMax = 0
  var nbSequences = 0

  var itemSupportCounter: scala.collection.mutable.Map[Int, Int] =
    scala.collection.mutable.Map[Int, Int]()

  def getLastItemFromPrefix(): Array[Int] = {
    prefix.drop(prefix.lastIndexOf(separator) + 1)
  }
  /**
   * Removes first element before first 0
   * Then count support for each item, so that unsupported item can be removed in the future.
   *
   * @return the inputed postfixes, desencapsulated and with the first elem removed
   */
  def preProcessPostfixes(postfixes: Array[Postfix]):
    Array[Array[Int]] = {

    val preprocessed = scala.collection.mutable.ArrayBuffer
      .empty[Array[Int]]
    itemSupportCounter = collection.mutable.Map[Int, Int]()

    // Count support for each item
    val itemSupportedByThisSequence = collection.mutable.Map[Int, Int]()
    for (postfix <- postfixes) {

      postfix.items.foreach(x => {
        itemSupportedByThisSequence.update(x, 1)
      })

      itemSupportedByThisSequence.keysIterator.foreach(x =>
        itemSupportCounter.update(x, itemSupportCounter.getOrElse(x, 0) + 1)
      )
      itemSupportedByThisSequence.clear()
    }

    // Filter unsupported item, make sure there are no consecutive zeros
    itemSupportCounter = itemSupportCounter.retain((k, v) => v >= minSup)
    // Remove unsupported item
    for (postfix <- postfixes) {
      // Create SDB postfix
      var lastItemWasEpsilon = false
      val curPostfix = scala.collection.mutable.ArrayBuffer.empty[Int]

      var notSkippedFirst = true
      var addedSomethingElseThanZero = false
      postfix.items.foreach(x => {
        if (notSkippedFirst) notSkippedFirst = false
        else if (x == 0) {
          if (!lastItemWasEpsilon) {
            lastItemWasEpsilon = true
            curPostfix.append(x)
          }
        }
        else if (itemSupportCounter.getOrElse(x, 0) >= minSup) {
          lastItemWasEpsilon = false
          addedSomethingElseThanZero = true
          curPostfix.append(x)
        }
      })
      // If sequence is useful
      if (addedSomethingElseThanZero) {
        // Check if larger than current lenSeqMax
        if (curPostfix.length > lenSeqMax) lenSeqMax = curPostfix.length
        // Append and continue
        preprocessed.append(curPostfix.toArray)
      }
    }
    preprocessed.toArray
  }

  def initCPVariables(maxPatternLength: Int,
                      itemSet : scala.collection.Set[Int],
                      outPutItemSetPermutation: Boolean):
    Array[CPIntVar] = {

    val nonZeroSet = itemSet -- Set(separator)
    val nonZeroEpsilonSet = nonZeroSet ++ Set(epsilon)
    val epsilonSet = itemSet ++ Set(epsilon)
    val specialValSet = Set(separator, epsilon)

    val maxLength = {
      if (!outPutItemSetPermutation && maxPatternLength < lenSeqMax) {
        math.min((maxPatternLength*2) + 1, lenSeqMax)
      }
      else lenSeqMax
    }

    // TODO : actual min pattern length calculation
    val minLength = if (outPutItemSetPermutation) 2
                    else 3

    val CPVariables = new Array[CPIntVar](maxLength)
    for (i <- CPVariables.indices) {
      CPVariables(i) = {
        if (outPutItemSetPermutation) {
          if (i < minLength) {
            CPIntVar.sparse(itemSet)
          }
          else CPIntVar.sparse(epsilonSet)
        }
        else {
          if (i % 2 == 0) {
            if (i < minLength) CPIntVar.apply(separator)
            else CPIntVar.sparse(specialValSet)
          }
          else if (i < minLength) CPIntVar.sparse(nonZeroSet)
          else CPIntVar.sparse(nonZeroEpsilonSet)
        }
      }
    }

    return CPVariables
  }

  def run(postfixes: Array[Postfix]): Iterator[(Array[Int], Long)] = {

    // If nothing to find, nothing to search
    if (maxPatternLength < 1) return Iterator()

    // Init class variables
    separator = 0
    epsilon = -1
    lenSeqMax = 0
    itemSupportCounter = scala.collection.mutable.Map[Int, Int]()

    // INIT sdb
    val sdb = preProcessPostfixes(postfixes)
    // IF we only have epsilon and separator, no need to search
    if(sdb.length < minSup) return Iterator()

    val itemSet = itemSupportCounter.keySet

    // Init CPvariables
    val CPVariables = initCPVariables(maxPatternLength, itemSet, outPutItemSetPermutation)

    // Setup max pattern length
    val amongItem = (itemSet -- Set(separator)).toSet
    if (maxPatternLength <= math.floor(lenSeqMax/2)) {
      add(atMost(maxPatternLength, CPVariables, amongItem))
    }

    val lastPrefixItemElems = getLastItemFromPrefix()

    // PRINT
    /*
    println(itemSet)
    println("Postfixes")
    postfixes.foreach(x => {x.items.foreach(x=> {print(x); print(" ")}); println()})
    println("sdb")
    sdb.foreach(x=> {x.foreach(x=> {print(x); print(" ")}); println()})
    println("itemSupportCounter")
    for(elem <- itemSet){
      println(elem + " : " + itemSupportCounter.getOrElse(elem, 0))
    }
    println("Variables")
    println("LenSeqMax : " + lenSeqMax)
    println("MinSup : " + minSup)
    println("cp var")
    println(CPVariables.mkString(" "))
    println()
    */

    // RUN

    val solutions = scala.collection.mutable.ArrayBuffer.empty[(Array[Int], Long)]
    val c = new sparkCP(CPVariables, sdb, lastPrefixItemElems, minSup)
    try {
      add(c)
    } catch {
      case noSol: oscar.cp.core.NoSolutionException => return Iterator()
      case e: Exception => throw e
    }

    this.solver.onSolution {
      val curSol = scala.collection.mutable.ArrayBuffer.empty[Int]
      CPVariables.foreach(x => {
        if (x.value != epsilon)curSol += x.value
      })
      solutions += ((curSol.toArray, c.curPrefixSupport))
    }


    this.solver.search(binaryStatic(CPVariables))
    this.solver.start()
    solutions.toIterator
  }
}

class sparkCP(val P: Array[CPIntVar],
              val SDB: Array[Array[Int]],
              val prefixOfLastItem: Array[Int],
              val minsup : Long)
  extends Constraint(P(0).store, "PPIC-SPARK") {

  // Define separator
  val separator = 0
  val epsilon = -1

  // Support for current prefix
  var curPrefixSupport : Int = 0

  // current position in P $P_i = P[curPosInP.value]$
  private[this] val curPosInP = new ReversibleInt(s, 0)

  // Map containing unsupported elements
  private[this] val supportMap = scala.collection.mutable.Map.empty[Int, Int]

  private[this] val itemSupportedByThisSequence = collection.mutable.Map[Int, Int]()

  // Array with start position in each sequence
  private[this] val posList = Array.fill[ReversibleInt](SDB.length)(new ReversibleInt(s, 0))

  /**
   * Entry in constraint, function for all init
   *
   * @param l
   * @return The outcome of the first propagation and consistency check
   */
  final override def setup(l: CPPropagStrength): CPOutcome = {
    if (propagate() == Failure) Failure
    else {
      var i = P.length
      while (i > 0) {
        i -= 1
        P(i).callPropagateWhenBind(this)
      }
      Suspend
    }
  }

  /**
   * propagate
   * @return the outcome i.e. Failure, Success or Suspend
   */
  final override def propagate(): CPOutcome = {
    var v = curPosInP.value

    // IF v > P.length, check is sequence is correct
    if (v >= P.length) {
      if (P(P.length-1).isBoundTo(separator) || P(P.length-1).isBoundTo(epsilon)) {
        return Success
      }
      else return Failure
    }

    // A valid postfix always finishes by a 0
    // And contain at least one, non zero, elem
    if (P(v).isBoundTo(epsilon)) {
      if (v-1 <= 0) return Failure
      if (!P(v-1).isBoundTo(separator)) return Failure
      else {
        enforceEpsilonFrom(v)
        return Success
      }
    }

    if (v > 0 && P(v-1).min == epsilon) {
      return Failure
    }

    while (v < P.length && P(v).isBound && P(v).min != epsilon) {

      // Project prefix
      if (!project(v)) {
        // println("false : " + P.mkString(" "))
        return Failure
      }
      // println("true : " + P.mkString(" "))

      // Clean next V
      if(v < P.length - 1) {
        // Remove unsupported item in next V
        if (!supportMap.isEmpty) {
          for (value <- P(v + 1)) {
            if (value != separator &&
              value != epsilon &&
              supportMap.getOrElse(value, 0) < minsup) {

              P(v + 1).removeValue(value)
            }
          }
          supportMap.clear()
          // IF v+1 == epsilon, enforce epsilon
          if (P(v + 1).isBound && P(v + 1).value == epsilon) {
            if (P(v).value == separator) {
              enforceEpsilonFrom(v + 1)
              return Success
            }
            else {
              return Failure
            }
          }
        }
        // IF v != separator, v+1 != epsilon
        if (P(v).value != separator) {
          if (P(v + 1).removeValue(epsilon) == Failure) return Failure
        }
        // If v == separator, v+1 != separator
        else {
          if (v + 1 == P.length) return Success
          else {
            if (P(v + 1).removeValue(separator) == Failure) return Failure
          }
        }
      }

      // Search next item
      curPosInP.incr()
      v = curPosInP.value
    }
    Suspend
  }

  /**
   * when $P_i = epsilon$, then $P_i+1 = epsilon$
   * @param i current position in P
   */
  def enforceEpsilonFrom(i: Int): Unit = {
    var j = i
    while (j < P.length) {
      P(j).assign(epsilon)
      j += 1
    }
  }

  def getElemOfP(posInPrefix: Int): Int = {
    if (posInPrefix >= 0) P(posInPrefix).value
    else if (prefixOfLastItem.length > -posInPrefix) {
      prefixOfLastItem(-(posInPrefix + prefixOfLastItem.length))
    }
    else 0
  }

  def checkElemOfP(posInPrefix: Int, currentItem: Int): Boolean = {
    if (posInPrefix >= 0) currentItem == P(posInPrefix).value
    else currentItem == prefixOfLastItem(-(posInPrefix + prefixOfLastItem.length))
  }

  /**
   * Project a prefix and recalculate supportMap
   *
   * @param v, the position in P
   * @return true if prefix is supported, false otherwise
   */
  def project(v: Int): Boolean = {

    // Init var
    curPrefixSupport = 0
    val soughtItem = P(v).value

    // Check strictly increasing value in building item
    if (soughtItem != separator && soughtItem < getElemOfP(v-1)) return false

    // Start search
    for (sequenceIndex <- SDB.indices) {

      // Get current index for that sequence
      val index = posList(sequenceIndex)
      // Search cur sequence
      var i = index.value
      val curSeq = SDB(sequenceIndex)
      // Search in current item first, either for the new item or the end of the item
      while (i < curSeq.length &&
        curSeq(i) != separator &&
        curSeq(i) != soughtItem) i += 1

      // IF found increase prefix support
      if (i == curSeq.length) index.setValue(i)
      else if (curSeq(i) == soughtItem) {
        i += 1
        index.setValue(i)
        curPrefixSupport += 1
      }
      else {
        // Else, answer is a subsequent item
        // Search until the prefix is supporter or run out of seq

        // Find current prefix boundaries
        var posInP = v
        while (posInP >= 0 && P(posInP).value != separator) posInP -= 1
        if (posInP == -1) posInP = - prefixOfLastItem.length
        else posInP += 1

        // Search for prefix
        var posInPrefix = posInP
        while (i < curSeq.length && posInPrefix <= v) {
          val currentItem = curSeq(i)
          if (currentItem == separator) posInPrefix = posInP
          else if (checkElemOfP(posInPrefix, currentItem)) posInPrefix += 1
          i += 1
        }

        // Save new index
        index.setValue(i)
        if (i < curSeq.length) curPrefixSupport += 1
      }
      // Recalculate support map
      if (soughtItem == separator) {
        while (i < curSeq.length) {
          itemSupportedByThisSequence.update(curSeq(i), 1)
          i += 1
        }
        itemSupportedByThisSequence.keysIterator.foreach(x =>
          supportMap.update(x, supportMap.getOrElse(x, 0) + 1)
        )
        itemSupportedByThisSequence.clear()
      }
    }
    // Return result
    if (curPrefixSupport >= minsup) return true
    else false
  }
}