/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.recordbreaker.learnstructure2;

import scala.io.Source
import scala.math._
import scala.util.control._
import scala.collection.mutable._
import java.util.regex.Pattern
import RBTypes._

/**
 * Parse a file or String data into appropriately formatted tokens
 *
 * @TODO production usage requires a version that doesn't parse all tokens at once
 * @TODO token delimiters other than <space>
 */
object Parse {

  def parseFile(fileName: String): Chunks = {
    process(Source.fromFile(fileName, "UTF-8"))
  }

  def parseString(str: String): Chunks = {
    process(Source.fromString(str))
  }

  def parseFileWithoutMeta(fileName: String): ParsedChunks = {
    processWithoutMeta(Source.fromFile(fileName, "UTF-8"))
  }

  def parseStringWithoutMeta(str: String): ParsedChunks = {
    processWithoutMeta(Source.fromString(str))
  }

  def process(src: Source): Chunks = {
    processWithoutMeta(src).map(processAllMetaTokens)
  }

  /**
   * Process a given input source without tagging the meta tokens
   *
   * @param src source data to be tokenized
   * @return parsed tokens for the input source
   */
  def processWithoutMeta(src: Source): ParsedChunks = {
    val isEmptyLine = (x:String)=>x.trim().length > 0
    val nonEmptyLines = src.getLines().filter(isEmptyLine)
    return (for (line <- nonEmptyLines) yield processLine(line)).toList
  }

  /**
   * Mark meta tokens for a given collection of data
   * @param tokens tokens with parsed values
   */
  private def processAllMetaTokens(tokens: List[BaseType]): List[BaseType] = {
    var result = tokens
    val makeDelim = (x: String) => new POther() with ParsedValue[String] {val parsedValue=x}
    for (i <- Range(0, LHS_DELIMITERS.length)) {
      result = processMetaTokenPair(makeDelim(LHS_DELIMITERS(i)), makeDelim(RHS_DELIMITERS(i)), result)
    }
    return result
  }

  /**
   * Tokenize and parse a single line of data
   * @param rawLine the raw string data
   * @return collection of parsed values
   */
  private def processLine(rawLine: String): ParsedChunk = {
    val line = normalizeMetaTokens(rawLine)
    val isEmptyString = (x:String)=>x.trim().length > 0
    val tokens = line.split(" ").filter(isEmptyString)
    return (for (t <- tokens) yield processToken(t)).toList
  }

  private val LHS_DELIMITERS = Vector[String]("<", "[", "(")
  private val RHS_DELIMITERS = Vector[String](">", "]", ")")

  /**
   * Wrap meta tokens with spaces
   */
  private def normalizeMetaTokens(rawLine: String): String = {
    var line = rawLine
    for (delim <- (LHS_DELIMITERS ++ RHS_DELIMITERS)) {
      val newVal = " " + delim + " "
      line = line.replaceAllLiterally(delim, newVal)
    }
    return line
  }

  /**
   * Convert a single string token to correctly typed value (int/double) if applicable
   */
  private def processToken(token: String): ParsedValue[Any] = {
    val value = token match {
      case x if token.forall(_.isDigit) => {
        try {
          new PInt() with ParsedValue[Int] {val parsedValue=x.toInt}
        } catch {
          case NonFatal(exc) => new PAlphanum() with ParsedValue[String] {val parsedValue=x}
        }
      }
      case y if {try{Some(y.toDouble); true} catch {case _:Throwable => false}} => {
        new PFloat() with ParsedValue[Double] {val parsedValue=y.toDouble}
      }
      case a if a.length() == 1 && Pattern.matches("\\p{Punct}", a) => {
        new POther() with ParsedValue[String] {val parsedValue=a}
      }
      case u => new PAlphanum() with ParsedValue[String] {val parsedValue=u}
    }
    return value
  }

  /**
   * Find given enclosing meta tokens and correctly mark them in the data
   * @param lhs the opening meta token
   * @param rhs the closing meta token
   * @param line tokens parsed from a single line in the file
   * @return normalized tokens for the line where properly formatted meta tokens are marked
   */
  private def processMetaTokenPair(lhs: POther with ParsedValue[String],
                       rhs: POther with ParsedValue[String],
                       line: List[BaseType]): List[BaseType] = {
    val (left, toProcess) = line.span(x => x != lhs)
    val (center, rest) = toProcess.slice(1,toProcess.length).span(x => x != rhs)
    if (center.length > 0) {
      left ++ List(PMetaToken(lhs, center, rhs)) ++ processMetaTokenPair(lhs, rhs, rest.slice(1, rest.length))
    } else {
      line
    }
  }

}


