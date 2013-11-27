//HELPER FUNCTIONS AND STRUCTURES

Geocoder_geocode_args = function(args){
this.r = new GeocodeRequest()
if( args != null ){if (null != args.r)
this.r = args.r
}}
Geocoder_geocode_args.prototype = {}
Geocoder_geocode_args.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 1:if (ftype == Thrift.Type.STRUCT) {
this.r = new GeocodeRequest()
this.r.read(input)
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

Geocoder_geocode_args.prototype.write = function(output){ 
output.writeStructBegin('Geocoder_geocode_args')
if (null != this.r) {
output.writeFieldBegin('r', Thrift.Type.STRUCT, 1)
this.r.write(output)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

Geocoder_geocode_result = function(args){
this.success = new GeocodeResponse()
if( args != null ){if (null != args.success)
this.success = args.success
}}
Geocoder_geocode_result.prototype = {}
Geocoder_geocode_result.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 0:if (ftype == Thrift.Type.STRUCT) {
this.success = new GeocodeResponse()
this.success.read(input)
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

Geocoder_geocode_result.prototype.write = function(output){ 
output.writeStructBegin('Geocoder_geocode_result')
if (null != this.success) {
output.writeFieldBegin('success', Thrift.Type.STRUCT, 0)
this.success.write(output)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

Geocoder_reverseGeocode_args = function(args){
this.r = new GeocodeRequest()
if( args != null ){if (null != args.r)
this.r = args.r
}}
Geocoder_reverseGeocode_args.prototype = {}
Geocoder_reverseGeocode_args.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 1:if (ftype == Thrift.Type.STRUCT) {
this.r = new GeocodeRequest()
this.r.read(input)
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

Geocoder_reverseGeocode_args.prototype.write = function(output){ 
output.writeStructBegin('Geocoder_reverseGeocode_args')
if (null != this.r) {
output.writeFieldBegin('r', Thrift.Type.STRUCT, 1)
this.r.write(output)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

Geocoder_reverseGeocode_result = function(args){
this.success = new GeocodeResponse()
if( args != null ){if (null != args.success)
this.success = args.success
}}
Geocoder_reverseGeocode_result.prototype = {}
Geocoder_reverseGeocode_result.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 0:if (ftype == Thrift.Type.STRUCT) {
this.success = new GeocodeResponse()
this.success.read(input)
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

Geocoder_reverseGeocode_result.prototype.write = function(output){ 
output.writeStructBegin('Geocoder_reverseGeocode_result')
if (null != this.success) {
output.writeFieldBegin('success', Thrift.Type.STRUCT, 0)
this.success.write(output)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

Geocoder_bulkReverseGeocode_args = function(args){
this.r = new BulkReverseGeocodeRequest()
if( args != null ){if (null != args.r)
this.r = args.r
}}
Geocoder_bulkReverseGeocode_args.prototype = {}
Geocoder_bulkReverseGeocode_args.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 1:if (ftype == Thrift.Type.STRUCT) {
this.r = new BulkReverseGeocodeRequest()
this.r.read(input)
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

Geocoder_bulkReverseGeocode_args.prototype.write = function(output){ 
output.writeStructBegin('Geocoder_bulkReverseGeocode_args')
if (null != this.r) {
output.writeFieldBegin('r', Thrift.Type.STRUCT, 1)
this.r.write(output)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

Geocoder_bulkReverseGeocode_result = function(args){
this.success = new BulkReverseGeocodeResponse()
if( args != null ){if (null != args.success)
this.success = args.success
}}
Geocoder_bulkReverseGeocode_result.prototype = {}
Geocoder_bulkReverseGeocode_result.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 0:if (ftype == Thrift.Type.STRUCT) {
this.success = new BulkReverseGeocodeResponse()
this.success.read(input)
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

Geocoder_bulkReverseGeocode_result.prototype.write = function(output){ 
output.writeStructBegin('Geocoder_bulkReverseGeocode_result')
if (null != this.success) {
output.writeFieldBegin('success', Thrift.Type.STRUCT, 0)
this.success.write(output)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

Geocoder_bulkSlugLookup_args = function(args){
this.r = new BulkSlugLookupRequest()
if( args != null ){if (null != args.r)
this.r = args.r
}}
Geocoder_bulkSlugLookup_args.prototype = {}
Geocoder_bulkSlugLookup_args.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 1:if (ftype == Thrift.Type.STRUCT) {
this.r = new BulkSlugLookupRequest()
this.r.read(input)
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

Geocoder_bulkSlugLookup_args.prototype.write = function(output){ 
output.writeStructBegin('Geocoder_bulkSlugLookup_args')
if (null != this.r) {
output.writeFieldBegin('r', Thrift.Type.STRUCT, 1)
this.r.write(output)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

Geocoder_bulkSlugLookup_result = function(args){
this.success = new BulkSlugLookupResponse()
if( args != null ){if (null != args.success)
this.success = args.success
}}
Geocoder_bulkSlugLookup_result.prototype = {}
Geocoder_bulkSlugLookup_result.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 0:if (ftype == Thrift.Type.STRUCT) {
this.success = new BulkSlugLookupResponse()
this.success.read(input)
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

Geocoder_bulkSlugLookup_result.prototype.write = function(output){ 
output.writeStructBegin('Geocoder_bulkSlugLookup_result')
if (null != this.success) {
output.writeFieldBegin('success', Thrift.Type.STRUCT, 0)
this.success.write(output)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

GeocoderClient = function(input, output) {
  this.input  = input
  this.output = null == output ? input : output
  this.seqid  = 0
}
GeocoderClient.prototype = {}
GeocoderClient.prototype.geocode = function(r){
this.send_geocode(r)
return this.recv_geocode()
}

GeocoderClient.prototype.send_geocode = function(r){
this.output.writeMessageBegin('geocode', Thrift.MessageType.CALL, this.seqid)
var args = new Geocoder_geocode_args()
args.r = r
args.write(this.output)
this.output.writeMessageEnd()
return this.output.getTransport().flush()
}

GeocoderClient.prototype.recv_geocode = function(){
var ret = this.input.readMessageBegin()
var fname = ret.fname
var mtype = ret.mtype
var rseqid= ret.rseqid
if (mtype == Thrift.MessageType.EXCEPTION) {
  var x = new Thrift.ApplicationException()
  x.read(this.input)
  this.input.readMessageEnd()
  throw x
}
var result = new Geocoder_geocode_result()
result.read(this.input)
this.input.readMessageEnd()

if (null != result.success ) {
  return result.success
}
throw "geocode failed: unknown result"
}
GeocoderClient.prototype.reverseGeocode = function(r){
this.send_reverseGeocode(r)
return this.recv_reverseGeocode()
}

GeocoderClient.prototype.send_reverseGeocode = function(r){
this.output.writeMessageBegin('reverseGeocode', Thrift.MessageType.CALL, this.seqid)
var args = new Geocoder_reverseGeocode_args()
args.r = r
args.write(this.output)
this.output.writeMessageEnd()
return this.output.getTransport().flush()
}

GeocoderClient.prototype.recv_reverseGeocode = function(){
var ret = this.input.readMessageBegin()
var fname = ret.fname
var mtype = ret.mtype
var rseqid= ret.rseqid
if (mtype == Thrift.MessageType.EXCEPTION) {
  var x = new Thrift.ApplicationException()
  x.read(this.input)
  this.input.readMessageEnd()
  throw x
}
var result = new Geocoder_reverseGeocode_result()
result.read(this.input)
this.input.readMessageEnd()

if (null != result.success ) {
  return result.success
}
throw "reverseGeocode failed: unknown result"
}
GeocoderClient.prototype.bulkReverseGeocode = function(r){
this.send_bulkReverseGeocode(r)
return this.recv_bulkReverseGeocode()
}

GeocoderClient.prototype.send_bulkReverseGeocode = function(r){
this.output.writeMessageBegin('bulkReverseGeocode', Thrift.MessageType.CALL, this.seqid)
var args = new Geocoder_bulkReverseGeocode_args()
args.r = r
args.write(this.output)
this.output.writeMessageEnd()
return this.output.getTransport().flush()
}

GeocoderClient.prototype.recv_bulkReverseGeocode = function(){
var ret = this.input.readMessageBegin()
var fname = ret.fname
var mtype = ret.mtype
var rseqid= ret.rseqid
if (mtype == Thrift.MessageType.EXCEPTION) {
  var x = new Thrift.ApplicationException()
  x.read(this.input)
  this.input.readMessageEnd()
  throw x
}
var result = new Geocoder_bulkReverseGeocode_result()
result.read(this.input)
this.input.readMessageEnd()

if (null != result.success ) {
  return result.success
}
throw "bulkReverseGeocode failed: unknown result"
}
GeocoderClient.prototype.bulkSlugLookup = function(r){
this.send_bulkSlugLookup(r)
return this.recv_bulkSlugLookup()
}

GeocoderClient.prototype.send_bulkSlugLookup = function(r){
this.output.writeMessageBegin('bulkSlugLookup', Thrift.MessageType.CALL, this.seqid)
var args = new Geocoder_bulkSlugLookup_args()
args.r = r
args.write(this.output)
this.output.writeMessageEnd()
return this.output.getTransport().flush()
}

GeocoderClient.prototype.recv_bulkSlugLookup = function(){
var ret = this.input.readMessageBegin()
var fname = ret.fname
var mtype = ret.mtype
var rseqid= ret.rseqid
if (mtype == Thrift.MessageType.EXCEPTION) {
  var x = new Thrift.ApplicationException()
  x.read(this.input)
  this.input.readMessageEnd()
  throw x
}
var result = new Geocoder_bulkSlugLookup_result()
result.read(this.input)
this.input.readMessageEnd()

if (null != result.success ) {
  return result.success
}
throw "bulkSlugLookup failed: unknown result"
}
