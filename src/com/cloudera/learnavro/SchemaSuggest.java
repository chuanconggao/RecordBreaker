// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.io.*;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/****************************************************************
 * SchemaSuggest generates labels for an Avro file with schema elts that are not
 * usefully named.  It uses a dictionary of schemas/data with high-quality labels.
 * It compares the candidate avro data to everything in the dictionary, finding the
 * k most-similar entries.  It then computes a mapping between the candidate schema
 * and each of the k best ones.  The user can then use the resulting schema to replace
 * the candidate's badly-labeled one.
 *
 * This class is particularly useful when operating on an Avro schema that was
 * algorithmically-generated (\ie, through learn-avro).
 *
 * @author mjc
 ****************************************************************/
public class SchemaSuggest {
  SchemaDictionary dict;

  /**
   * Load in the Schema Dictionary from the indicated file.
   */
  public SchemaSuggest(File dataDir) throws IOException {
    this.dict = new SchemaDictionary(dataDir);
  }

  /**
   * This method infers new schema labels for each element in the input.  It returns a Schema object that
   * has the identical format as the input file's Schema object, but the labels may be changed.
   */
  public List<DictionaryMapping> inferSchemaMapping(File avroFile, int k) throws IOException {
    System.err.println("Observed filename: " + avroFile);

    SchemaStatisticalSummary srcSummary = new SchemaStatisticalSummary();
    Schema srcSchema = null;
    try {
      srcSchema = srcSummary.createSummaryFromData(avroFile);
    } finally {
    }

    //
    // Compare the statistics to the database of schema statistics.  Find the closest matches, both
    // on a per-attribute basis and structurally.
    //
    TreeSet<DictionaryMapping> sorter = new TreeSet<DictionaryMapping>();
    for (SchemaDictionaryEntry elt: dict.contents()) {
      SchemaMapping mapping = srcSummary.getBestMapping(elt.getSummary());
      sorter.add(new DictionaryMapping(mapping, elt));
    }

    // Return the k best schema mappings
    List<DictionaryMapping> dsts = new ArrayList<DictionaryMapping>();
    for (DictionaryMapping dp: sorter) {
      dsts.add(dp);
      if (dsts.size() >= k) {
        break;
      }
    }

    return dsts;
  }

  /**
   * SchemaSuggest takes an avro file where schema elements may be anonymous.  It then attempts to 
   * compute good labels for the anonymous elts.  By default, this tool simply prints out the
   * suggested labels, if any.  The user may include a flag to rewrite the input data using
   * the new labels.
   *
   * schemaSuggest avroFile 
   *
   */
  public static void main(String argv[]) throws IOException {
    CommandLine cmd = null;
    Options options = new Options();
    options.addOption("?", false, "Help for command-line");
    options.addOption("f", true, "Accept suggestions and rewrite input to a new Avro file");
    options.addOption("k", true, "How many matches to emit.");

    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaSuggest", options, true);
      System.exit(-1);
    }

    if (cmd.hasOption("?")) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaSuggest", options, true);
      System.exit(0);
    }

    int k = 5;
    if (cmd.hasOption("k")) {
      try {
        k = Integer.parseInt(cmd.getOptionValue("k"));
      } catch (NumberFormatException nfe) {
      }
    }

    String[] argArray = cmd.getArgs();
    if (argArray.length == 0) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaSuggest", options, true);
      System.exit(0);
    }

    File dataDir = new File(argArray[0]).getCanonicalFile();
    File inputData = new File(argArray[1]).getCanonicalFile();
    SchemaSuggest ss = new SchemaSuggest(dataDir);
    List<DictionaryMapping> mappings = ss.inferSchemaMapping(inputData, k);

    if (! cmd.hasOption("f")) {
      System.out.println("Inferred schema mappings:");
      int counter = 1;
      for (DictionaryMapping mapping: mappings) {
        SchemaMapping sm = mapping.getMapping();
        List<SchemaMappingOp> bestOps = sm.getMapping();

        int counterIn = 1;
        System.err.println();
        System.out.println(counter + ".  Mapping to '" + mapping.getDictEntry().getInfo() + "' has " + bestOps.size() + " ops and distance " + sm.getDist());
        for (SchemaMappingOp op: bestOps) {
          System.err.println("\t" + counterIn + ".  " + "op: " + op);
          counterIn++;
        }
        counter++;
      }
    }
  }
}