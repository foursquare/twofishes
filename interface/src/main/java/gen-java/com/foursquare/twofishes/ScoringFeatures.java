/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.foursquare.twofishes;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

// No additional import required for struct/union.

public class ScoringFeatures implements TBase<ScoringFeatures, ScoringFeatures._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("ScoringFeatures");

  private static final TField POPULATION_FIELD_DESC = new TField("population", TType.I32, (short)1);
  private static final TField BOOST_FIELD_DESC = new TField("boost", TType.I32, (short)2);
  private static final TField PARENTS_FIELD_DESC = new TField("parents", TType.LIST, (short)3);
  private static final TField CAN_GEOCODE_FIELD_DESC = new TField("canGeocode", TType.BOOL, (short)5);

  public int population;
  public int boost;
  public List<String> parents;
  public boolean canGeocode;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    POPULATION((short)1, "population"),
    BOOST((short)2, "boost"),
    PARENTS((short)3, "parents"),
    CAN_GEOCODE((short)5, "canGeocode");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // POPULATION
          return POPULATION;
        case 2: // BOOST
          return BOOST;
        case 3: // PARENTS
          return PARENTS;
        case 5: // CAN_GEOCODE
          return CAN_GEOCODE;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __POPULATION_ISSET_ID = 0;
  private static final int __BOOST_ISSET_ID = 1;
  private static final int __CANGEOCODE_ISSET_ID = 2;
  private BitSet __isset_bit_vector = new BitSet(3);

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.POPULATION, new FieldMetaData("population", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.I32)));
    tmpMap.put(_Fields.BOOST, new FieldMetaData("boost", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.I32)));
    tmpMap.put(_Fields.PARENTS, new FieldMetaData("parents", TFieldRequirementType.OPTIONAL, 
        new ListMetaData(TType.LIST, 
            new FieldValueMetaData(TType.STRING))));
    tmpMap.put(_Fields.CAN_GEOCODE, new FieldMetaData("canGeocode", TFieldRequirementType.OPTIONAL, 
        new FieldValueMetaData(TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(ScoringFeatures.class, metaDataMap);
  }

  public ScoringFeatures() {
    this.population = 0;

    this.boost = 0;

    this.parents = new ArrayList<String>();

    this.canGeocode = true;

  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ScoringFeatures(ScoringFeatures other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.population = other.population;
    this.boost = other.boost;
    if (other.isSetParents()) {
      List<String> __this__parents = new ArrayList<String>();
      for (String other_element : other.parents) {
        __this__parents.add(other_element);
      }
      this.parents = __this__parents;
    }
    this.canGeocode = other.canGeocode;
  }

  public ScoringFeatures deepCopy() {
    return new ScoringFeatures(this);
  }

  @Override
  public void clear() {
    this.population = 0;

    this.boost = 0;

    this.parents = new ArrayList<String>();

    this.canGeocode = true;

  }

  public int getPopulation() {
    return this.population;
  }

  public ScoringFeatures setPopulation(int population) {
    this.population = population;
    setPopulationIsSet(true);
    return this;
  }

  public void unsetPopulation() {
    __isset_bit_vector.clear(__POPULATION_ISSET_ID);
  }

  /** Returns true if field population is set (has been asigned a value) and false otherwise */
  public boolean isSetPopulation() {
    return __isset_bit_vector.get(__POPULATION_ISSET_ID);
  }

  public void setPopulationIsSet(boolean value) {
    __isset_bit_vector.set(__POPULATION_ISSET_ID, value);
  }

  public int getBoost() {
    return this.boost;
  }

  public ScoringFeatures setBoost(int boost) {
    this.boost = boost;
    setBoostIsSet(true);
    return this;
  }

  public void unsetBoost() {
    __isset_bit_vector.clear(__BOOST_ISSET_ID);
  }

  /** Returns true if field boost is set (has been asigned a value) and false otherwise */
  public boolean isSetBoost() {
    return __isset_bit_vector.get(__BOOST_ISSET_ID);
  }

  public void setBoostIsSet(boolean value) {
    __isset_bit_vector.set(__BOOST_ISSET_ID, value);
  }

  public int getParentsSize() {
    return (this.parents == null) ? 0 : this.parents.size();
  }

  public java.util.Iterator<String> getParentsIterator() {
    return (this.parents == null) ? null : this.parents.iterator();
  }

  public void addToParents(String elem) {
    if (this.parents == null) {
      this.parents = new ArrayList<String>();
    }
    this.parents.add(elem);
  }

  public List<String> getParents() {
    return this.parents;
  }

  public ScoringFeatures setParents(List<String> parents) {
    this.parents = parents;
    return this;
  }

  public void unsetParents() {
    this.parents = null;
  }

  /** Returns true if field parents is set (has been asigned a value) and false otherwise */
  public boolean isSetParents() {
    return this.parents != null;
  }

  public void setParentsIsSet(boolean value) {
    if (!value) {
      this.parents = null;
    }
  }

  public boolean isCanGeocode() {
    return this.canGeocode;
  }

  public ScoringFeatures setCanGeocode(boolean canGeocode) {
    this.canGeocode = canGeocode;
    setCanGeocodeIsSet(true);
    return this;
  }

  public void unsetCanGeocode() {
    __isset_bit_vector.clear(__CANGEOCODE_ISSET_ID);
  }

  /** Returns true if field canGeocode is set (has been asigned a value) and false otherwise */
  public boolean isSetCanGeocode() {
    return __isset_bit_vector.get(__CANGEOCODE_ISSET_ID);
  }

  public void setCanGeocodeIsSet(boolean value) {
    __isset_bit_vector.set(__CANGEOCODE_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case POPULATION:
      if (value == null) {
        unsetPopulation();
      } else {
        setPopulation((Integer)value);
      }
      break;

    case BOOST:
      if (value == null) {
        unsetBoost();
      } else {
        setBoost((Integer)value);
      }
      break;

    case PARENTS:
      if (value == null) {
        unsetParents();
      } else {
        setParents((List<String>)value);
      }
      break;

    case CAN_GEOCODE:
      if (value == null) {
        unsetCanGeocode();
      } else {
        setCanGeocode((Boolean)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case POPULATION:
      return new Integer(getPopulation());

    case BOOST:
      return new Integer(getBoost());

    case PARENTS:
      return getParents();

    case CAN_GEOCODE:
      return new Boolean(isCanGeocode());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case POPULATION:
      return isSetPopulation();
    case BOOST:
      return isSetBoost();
    case PARENTS:
      return isSetParents();
    case CAN_GEOCODE:
      return isSetCanGeocode();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ScoringFeatures)
      return this.equals((ScoringFeatures)that);
    return false;
  }

  public boolean equals(ScoringFeatures that) {
    if (that == null)
      return false;

    boolean this_present_population = true && this.isSetPopulation();
    boolean that_present_population = true && that.isSetPopulation();
    if (this_present_population || that_present_population) {
      if (!(this_present_population && that_present_population))
        return false;
      if (this.population != that.population)
        return false;
    }

    boolean this_present_boost = true && this.isSetBoost();
    boolean that_present_boost = true && that.isSetBoost();
    if (this_present_boost || that_present_boost) {
      if (!(this_present_boost && that_present_boost))
        return false;
      if (this.boost != that.boost)
        return false;
    }

    boolean this_present_parents = true && this.isSetParents();
    boolean that_present_parents = true && that.isSetParents();
    if (this_present_parents || that_present_parents) {
      if (!(this_present_parents && that_present_parents))
        return false;
      if (!this.parents.equals(that.parents))
        return false;
    }

    boolean this_present_canGeocode = true && this.isSetCanGeocode();
    boolean that_present_canGeocode = true && that.isSetCanGeocode();
    if (this_present_canGeocode || that_present_canGeocode) {
      if (!(this_present_canGeocode && that_present_canGeocode))
        return false;
      if (this.canGeocode != that.canGeocode)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(ScoringFeatures other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    ScoringFeatures typedOther = (ScoringFeatures)other;

    lastComparison = Boolean.valueOf(isSetPopulation()).compareTo(typedOther.isSetPopulation());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPopulation()) {
      lastComparison = TBaseHelper.compareTo(this.population, typedOther.population);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetBoost()).compareTo(typedOther.isSetBoost());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBoost()) {
      lastComparison = TBaseHelper.compareTo(this.boost, typedOther.boost);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetParents()).compareTo(typedOther.isSetParents());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetParents()) {
      lastComparison = TBaseHelper.compareTo(this.parents, typedOther.parents);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCanGeocode()).compareTo(typedOther.isSetCanGeocode());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCanGeocode()) {
      lastComparison = TBaseHelper.compareTo(this.canGeocode, typedOther.canGeocode);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // POPULATION
          if (field.type == TType.I32) {
            this.population = iprot.readI32();
            setPopulationIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // BOOST
          if (field.type == TType.I32) {
            this.boost = iprot.readI32();
            setBoostIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3: // PARENTS
          if (field.type == TType.LIST) {
            {
              TList _list4 = iprot.readListBegin();
              this.parents = new ArrayList<String>(_list4.size);
              for (int _i5 = 0; _i5 < _list4.size; ++_i5)
              {
                String _elem6;
                _elem6 = iprot.readString();
                this.parents.add(_elem6);
              }
              iprot.readListEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 5: // CAN_GEOCODE
          if (field.type == TType.BOOL) {
            this.canGeocode = iprot.readBool();
            setCanGeocodeIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (isSetPopulation()) {
      oprot.writeFieldBegin(POPULATION_FIELD_DESC);
      oprot.writeI32(this.population);
      oprot.writeFieldEnd();
    }
    if (isSetBoost()) {
      oprot.writeFieldBegin(BOOST_FIELD_DESC);
      oprot.writeI32(this.boost);
      oprot.writeFieldEnd();
    }
    if (this.parents != null) {
      if (isSetParents()) {
        oprot.writeFieldBegin(PARENTS_FIELD_DESC);
        {
          oprot.writeListBegin(new TList(TType.STRING, this.parents.size()));
          for (String _iter7 : this.parents)
          {
            oprot.writeString(_iter7);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
    }
    if (isSetCanGeocode()) {
      oprot.writeFieldBegin(CAN_GEOCODE_FIELD_DESC);
      oprot.writeBool(this.canGeocode);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ScoringFeatures(");
    boolean first = true;

    if (isSetPopulation()) {
      sb.append("population:");
      sb.append(this.population);
      first = false;
    }
    if (isSetBoost()) {
      if (!first) sb.append(", ");
      sb.append("boost:");
      sb.append(this.boost);
      first = false;
    }
    if (isSetParents()) {
      if (!first) sb.append(", ");
      sb.append("parents:");
      if (this.parents == null) {
        sb.append("null");
      } else {
        sb.append(this.parents);
      }
      first = false;
    }
    if (isSetCanGeocode()) {
      if (!first) sb.append(", ");
      sb.append("canGeocode:");
      sb.append(this.canGeocode);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }

}

