#include "Algebra.h"
#include "NestedList.h"
#include "QueryProcessor.h"
#include "StandardTypes.h"
#include "Symbols.h"
#include "DateTime.h"
#include "Stream.h"
#include "TBTree.h"
#include <iostream>
#include <fstream>
#include <map>
#include "WinUnix.h"
#include "SecondoSystem.h"
#include "ListUtils.h"
#include "Attribute.h"
#include "DateTime.h"
#include "RTreeAlgebra.h"
#include "RectangleAlgebra.h"
#include "TemporalAlgebra.h"
#include "UploadUnit.h"
#include "Symbols.h"

extern NestedList     *nl;
extern QueryProcessor *qp;

/****************************************************************

    1.operator convertMP2UU2

***************************************************************/

// type map function
//
bool tuisMPoint = true;

ListExpr ConvertMP2UU2TM(ListExpr args)
{
    NList type(args);
    if ( !type.hasLength(4) )
    {
        return listutils::typeError("Expecting four arguments.");
    }

    NList first = type.first();
    if ( !first.hasLength(2)  ||
            !first.first().isSymbol(Symbol::STREAM()) ||
            !first.second().hasLength(2) ||
            !first.second().first().isSymbol(Tuple::BasicType()) ||
            !IsTupleDescription( first.second().second().listExpr() ))
    {
        return listutils::typeError("Error in first argument!");
    }

    if ( !nl->IsEqual(nl->Second(args), CcInt::BasicType()) )
    {
        return NList::typeError( "int for second argument expected!" );
    }

    NList third = type.third();
    if ( !third.isSymbol() )
    {
        return NList::typeError( "Attribute name for third argument expected!" );
    }

    NList fourth = type.fourth();
    if ( !fourth.isSymbol() )
    {
        return NList::typeError( "Attribute name for fourth argument expected!" );
    }

    string attrname1 = type.third().str();
    ListExpr attrtype1 = nl->Empty();
    int j = FindAttribute(first.second().second().listExpr(),attrname1,attrtype1);

    string attrname2 = type.fourth().str();
    ListExpr attrtype2 = nl->Empty();
    int k = FindAttribute(first.second().second().listExpr(),attrname2,attrtype2);

    if ( j != 0 )
    {
        if ( nl->SymbolValue(attrtype1) != CcInt::BasicType() )
        {
            return NList::typeError("First attribute type is not of type int.");
        }
    }
    else
    {
        return NList::typeError("Unknown attribute name '" + attrname1 + "' !");
    }

    if ( k != 0 )
    {
        if ( nl->SymbolValue(attrtype2) != "mpoint" &&
                nl->SymbolValue(attrtype2) != UPoint::BasicType())
        {
            return NList::typeError("Second attribute is not of type"
                    " mpoint or upoint.");
        }

        if ( nl->SymbolValue(attrtype2) == UPoint::BasicType()) tuisMPoint = false;
        else tuisMPoint = true;
    }
    else
    {
        return NList::typeError("Unknown attribute name '" + attrname2 + "' !");
    }

    // Create output list
    ListExpr tupleList = nl->TwoElemList(
            nl->TwoElemList(nl->SymbolAtom("Upload"),
                nl->SymbolAtom(UploadUnit::BasicType())),
            nl->TwoElemList(nl->SymbolAtom("Time"),
                nl->SymbolAtom(CcReal::BasicType())));
    ListExpr outputstream = nl->TwoElemList(nl->SymbolAtom(Symbol::STREAM()),
            nl->TwoElemList(nl->SymbolAtom(Tuple::BasicType()),
                tupleList));

    return NList( NList(Symbol::APPEND()),
            nl->TwoElemList(nl->IntAtom(j), nl->IntAtom(k)),
            outputstream ).listExpr();
}

//value map function
//
int ConvertMP2UU2VM(Word* args, Word& result, int message,
        Word& local, Supplier s)
{
    struct Iterator
    {
        Iterator()
        {
            it  = 0;
            cnt = 0;
        }
        int it;
        int cnt;
        Word currentTupleWord;
        TupleType *tupletype;
    };

    Iterator* iterator = static_cast<Iterator*>(local.addr);

    switch( message )
    {
        case OPEN:
            {
                iterator = new Iterator();
                local.addr = iterator;
                iterator->tupletype = new TupleType(nl->Second(GetTupleResultType(s)));
                qp->Open(args[0].addr);
                return 0;
            }
        case REQUEST:
            {
                int numUploads = static_cast<CcInt*>( args[1].addr )->GetIntval();
                int attr1 = static_cast<CcInt*>( args[4].addr )->GetIntval() - 1;
                int attr2 = static_cast<CcInt*>( args[5].addr )->GetIntval() - 1;

                bool received = true;

                if (iterator->it == 0)
                {
                    qp->Request( args[0].addr, iterator->currentTupleWord );
                    received = qp->Received(args[0].addr);
                }

                if ( received && iterator->cnt < numUploads )
                {
                    Tuple* currentTuple = static_cast<Tuple*>
                        ( iterator->currentTupleWord.addr );
                    int moID = static_cast<CcInt*>(currentTuple->GetAttribute(attr1))
                        ->GetIntval();
                    if (tuisMPoint)
                    {
                        MPoint* mp = static_cast<MPoint*>(currentTuple->GetAttribute(attr2));
                        UPoint up;
                        mp->Get(iterator->it,up);
                        UnitPos pos( up.p1.GetX(), up.p1.GetY() );
                        UploadUnit* uu = new UploadUnit(moID, up.timeInterval.end, pos );
                        CcReal *cctime = new CcReal(up.timeInterval.end.ToDouble());
                        Tuple* tup = new Tuple( iterator->tupletype );
                        tup->PutAttribute( 0, ( Attribute* ) uu );
                        tup->PutAttribute( 1, ( Attribute* ) cctime );
                        result.addr = tup;
                        iterator->it++;
                        iterator->cnt++;
                        if (iterator->it == mp->GetNoComponents()) iterator->it = 0;
                    }
                    else
                    {
                        UPoint* up = static_cast<UPoint*>(currentTuple->GetAttribute(attr2));
                        UnitPos pos( up->p1.GetX(), up->p1.GetY() );
                        UploadUnit* uu = new UploadUnit(moID, up->timeInterval.end, pos );
                        CcReal *cctime = new CcReal(up->timeInterval.end.ToDouble());
                        Tuple* tup = new Tuple( iterator->tupletype );
                        tup->PutAttribute( 0, ( Attribute* ) uu );
                        tup->PutAttribute( 1, ( Attribute* ) cctime );
                        result.addr = tup;
                        iterator->cnt++;
                    }
                    return YIELD;
                }
                else
                {
                    delete iterator;
                    local.addr  = 0;
                    result.addr = 0;
                    return CANCEL;
                }
            }
        case CLOSE:
            {
                return 0;
            }
        default:
            {
                return -1;
            }
    }
}

//
// operator info

struct ConvertMP2UU2Info : OperatorInfo {
    ConvertMP2UU2Info()
    {
        name      = "convertMP2UU2";
        signature = "((stream (tuple([a1:d1, ..., ai:int, ..., "
            "aj:mpoint|upoint, ..., an:dn]))) x int x ai x aj)"
            " -> stream (tuple (Upload uploadunit))";
        syntax    = "_ convertMP2UU [ _, _, _ ]";
        meaning   = "Converts mpoints into upload units.";
    }
};



/****************************************************************************
 
   Insert
 
 ***************************************************************************/
/*
//
// UpdateTrajectory
//
ListExpr UpdateTrajectoryTypeMap(ListExpr args)
{
    NList type(args);
    if(! type.hasLength(3)){
        return listutils::typeError("Expecting four arguments");
    }

    if(! nl->IsEqual(nl->First(args), Upload::BasicType())){
        return listutlis::typeError("upload unit for first argument expected!");
    }

    if(! listutils::isRelDescription(nl->Second(args))){
        return listutils::typeError("relation for second argument expected!");
    }

    string attrname = type.third().str();
    ListExpr attrtype = nl->Empty();
    int j = FindAttribute(nl->Second(nl->Second(args)), attrname, attrtype);
    
    if(j != 0){
        if(nl->SymbolValue(attrtype) != MPoint::BasicType())
        {
            return listutils::typeError("first attribute type is not type mpoint");
        }
    }else{
        return listutils::typeError("unknown attribute name " + attrname + " !");
    }
    
    //
    // create output list
    ListExpr resulttype = nl->SymbolAtom(CcBool::BasicType());
    return NList( NList(Symbol::APPEND()),
            NL->OneElemList(nl->IntAtom(j)),
            resulttype).listExpr();
}

//
//  UpdateTrajectory value map
//
int UpdateTrajectoryValueMap(Word *args, Word &result, int message, Word &local, Supplier s)
{    
    UploadUnit *unitptr = (UploadUnit*)args[0].addr;
    Relation *rel = (Relation*)args[1].addr;
    int attrindex = ((CcInt*)(args[3].addr))->GetValue()-1;

    
}
*/

/****************************************************************************
 
   TrajUpdate
 
 ***************************************************************************/
/*
TrajUpdate(const ListExpr typeInfo_current_pos, const ListExpr typeInfo_trajectories)
{
    // create a new relation of current position and a new relation storing all trajectories of objects
    // 
    current_pos = new Relation(typeInfo_current_pos);
    relDesc_current_pos = current_pos->get
    trajectories = new Relation(typeInfo_trajectories);
    
    // create btree and rtree
    //

}

TrajUpdate(TupleType *typeInfo_current_pos, TupleType *typeInfo_trajectories)
{

}

TrajUpdate::TrajUpdate(){
    cout<<"TrajUpdate::TrajUpdate()"<<endl;
    // Create SmiUpdateFile
    suf = new SmiUpdateFile(MyPageSize);
    suf->Create();
    fid = suf->GetFileId();

    // Init the TrajUpdate Header
    header = new TUHeader();
    size = header->size = 0;
    pagesize = header->pagesize = MyPageSize;

    // Create the header page
    SmiUpdatePage *headerpage;
    int AppendedPage = suf->AppendNewPage(headerpage);
    assert(AppendedPage);
    assert(1 == headerpage->GetPageNo());

    UpdateHeader();
}

TrajUpdate::TrajUpdate(SmiFileId fileid){
}

TrajUpdate::~TrajUpdate() {
    cout<<"TrajUpdate::~TrajUpdate"<<endl;
    //
}

// The mandatory set of algebra support function
//
Word TrajUpdate::In(const ListExpr typeinfo, const ListExpr instance, const int errorPos, ListExpr &errorInfo, bool & correct){
    cout<<"TrajUpdate::In"<<endl;
    correct = false;
    Word result = SetWord(Address(0));

    //check the list length
    if(nl->ListLength(instance) != 1){
        cmsg.inFunError("A list of length one expected!");
        return result;
    }

    //check string types
    ListExpr strlist = nl->First(instance);
    if( !(nl->ListLength(strlist) == 1 &&
                nl->IsAtom(nl->First(strlist)) &&
                nl->AtomType(nl->First(strlist)) == StringType))
    {
        cmsg.inFunError("A list of one string type values is expected for the first argument");
        return result;
    }
    string str = nl->StringValue(nl->First(strlist));
    
    result.addr = new TrajUpdate();
    ((TrajUpdate*)result.addr)->AddString(str);

    correct = true;
    return result;
}
//
//
ListExpr TrajUpdate::Out(ListExpr typeInfo, Word value){
    cout<<"TrajUpdate::Out"<<endl;
    TrajUpdate *tu = static_cast<TrajUpdate*>(value.addr);
    string str = tu->GetString();
    return nl->OneElemList(nl->StringAtom(str, true));
}
//
//
Word TrajUpdate::Create(const ListExpr typeinfo){
    cout<<"TrajUpdate::Create"<<endl;
    return SetWord(new TrajUpdate());
}
//
//
void TrajUpdate::Delete(const ListExpr typeinfo, Word &w){
    cout<<"TrajUpdate::Delete"<<endl;
    TrajUpdate *tu = static_cast<TrajUpdate *>(w.addr);
    if(tu->suf->IsOpen()) 
        tu->suf->Close();
    delete tu;
    w.addr = 0;
}
//
//
bool TrajUpdate::Open(SmiRecord &valueRecord, size_t &offset, const ListExpr typeinfo, Word &value){
    cout<<"TrajUpdate::Open"<<endl;
    SmiFileId fileid;
    //db_pgno_t headerPageNo;

    bool ok = true;
    ok = ok && valueRecord.Read(&fileid, sizeof(SmiFileId), offset);
    offset += sizeof(SmiFileId);
    TrajUpdate *tu = new TrajUpdate(fileid);
    value.addr = tu;
    return ok;
}
//
//
bool TrajUpdate::Save(SmiRecord &valueRecord, size_t &offset, const ListExpr typeinfo, Word &w){
    cout<<"TrajUpdate::Save"<<endl;
    TrajUpdate *tu = static_cast<TrajUpdate *>(w.addr);
    bool ok = true;
    SmiFileId fileid = tu->fid;
    ok =  ok && valueRecord.Write(&fileid, sizeof(SmiFileId), offset);
    offset +=  sizeof(SmiFileId);
    tu->suf->SyncFile();
    tu->suf->Close();
    return ok;
}
//
//
void TrajUpdate::Close(const ListExpr typeinfo, Word &w){
    cout<<"TrajUpdate::Close"<<endl;
    TrajUpdate *tu = static_cast<TrajUpdate *>(w.addr);
    delete tu;
    w.addr = 0;
}
//
//
Word TrajUpdate::Clone(const ListExpr typeinfo, const Word &w){
    cout<<"TrajUpdate::Clone"<<endl;
    return SetWord(Address(0));
}
//
//
bool TrajUpdate::KindCheck(ListExpr type, ListExpr &errorInfo){
    cout<<"TrajUpdate::KindCheck"<<endl;
    return (nl->IsEqual(type, TrajUpdate::BasicType()));
}
//
//
void* TrajUpdate::Cast(void *addr){
    cout<<"TrajUpdate::Cast"<<endl;
    return (0);
}
//
//
int TrajUpdate::SizeOfObj(){
    cout<<"TrajUpdate::SizeOfObj"<<endl;
    return sizeof(TrajUpdate);
}
//
//
ListExpr TrajUpdate::Property(){
    cout<<"TrajUpdate::Property"<<endl;
    return nl->TwoElemList(
                nl->FiveElemList(
                    nl->StringAtom("Signature"),
                    nl->StringAtom("Example Type List"),
                    nl->StringAtom("List Rep"),
                    nl->StringAtom("Example List"),
                    nl->StringAtom("Remarks")),
                nl->FiveElemList(
                    nl->StringAtom("-> DATA"),
                    nl->StringAtom(TrajUpdate::BasicType()),
                    nl->StringAtom("(string)"),
                    nl->StringAtom("(hello world)"),
                    nl->StringAtom("a string")));
}
//
//
*/

/****************************************************************

    1.operator GenUnitTrip

***************************************************************/

// type map function
// stream(tuple(Uploads uploadunit)) x btree(Id->TupleId) x rel(Id int, pos_x double, pos_y double, timestamp double)
// -> stream(tuple(Id int, UTrip upoint))
ListExpr GenUnitTripTM(ListExpr args)
{
    ListExpr first_stream, second_btree, third_rel;

    if(nl->ListLength(args) != 3){
        return listutils::typeError("three arguments expected!");
    }

    first_stream = nl->First(args);
    second_btree = nl->Second(args);
    third_rel = nl->Third(args);

    // check the uploadunit
    if(! listutils::isTupleStream(first_stream)){
        return listutils::typeError("first argument should be stream");
    }
    ListExpr uu = nl->Second(nl->First(first_stream));
    if(! listutils::isSymbol(uu, UploadUnit::BasicType())){
        return listutils::typeError("first argument should be UploadUnit");
    }

    // check the btree
    if( nl->IsAtom(second_btree)
            || (nl->ListLength(second_btree) != 3)){
        return listutils::typeError("second argument should be btree");
    }
    ListExpr btree_symbol = nl->First(second_btree);
    if(! nl->IsAtom(btree_symbol)
            || (nl->AtomType(btree_symbol) != SymbolType)
            || (nl->SymbolValue(btree_symbol) != BTree::BasicType())){
        return listutils::typeError("second argument should be btree");
    }

    // check the relation of current position
    if(! listutils::isRelDescription(third_rel)){
        return listutils::typeError("third argument should be relation");
    }

    //
    //
    //
    //
    //
    return nl->Empty();
}

//value map function
//
int GenUnitTripVM(Word* args, Word& result, int message, Word& local, Supplier s)
{

    switch( message )
    {
        case OPEN:
            {
            }
        case REQUEST:
            {
            }
        case CLOSE:
            {
                return 0;
            }
    }
    return 0;
}

//
// operator info
struct GenUnitTripInfo : OperatorInfo {
    GenUnitTripInfo()
    {
        name      = "genunittrip";
        signature = "((stream (tuple([a1:d1, ..., ai:int, ..., "
            "aj:uploaduint, ..., an:dn]))) x btree "
            "x rel( tuple( b1:int, b2:double, b3:double, b4:double)))"
            " -> stream (tuple (Id int, Utrip upoint))";
        syntax    = "_ genunittrip [ _, _ ]";
        meaning   = "generate a unit trip of type upoint from upload unit,"
            "btree and relation of current moving object position";
    }
};


/****************************************************************

    operator UpdateTrajectory

***************************************************************/

// type map function
// stream(tuple(Id int, UTrip upoint)) x rel(tuple(Id int, Trip mpoint)
// x btree x rtree x Id x UTrip x Trip
// -> bool
ListExpr UpdateTrajectoryTM(ListExpr args)
{
    ListExpr first, second, third, fourth, fifth, sixth, seventh;
    int i1, i2, i3, i4; //i1: Id, i2: UTrip, i3:Id, i4:Trip
    string attrname;
    ListExpr attrtype, attrlist;

    // check the number of arguments
    if(nl->ListLength(args) != 7){
        listutils::typeError("Seven arguments expected!");
    }

    //
    first = nl->First(args);      //stream
    second = nl->Second(args);      //relation
    third = nl->Third(args);        //btree
    fourth = nl->Fourth(args);      //rtree
    fifth = nl->Fifth(args);     //Id
    sixth = nl->Sixth(args);   //UTrip
    seventh = nl->Seventh(args);   //Trip

    //
    //
    // check the stream
    if(! listutils::isTupleStream(first)){
        return listutils::typeError("First argument should be stream!");
    }

    // check the relation
    if(! listutils::isRelDescription(second)){
        return listutils::typeError("Second argument should be relation!");
    }

    // check the btree
    if(nl->IsAtom(third) || nl->ListLength(third) != 3){
        return listutils::typeError("Third argument should be btree!");
    }
    ListExpr btreeSymbol = nl->First(third);
    if(! nl->IsAtom(btreeSymbol) 
            || (nl->AtomType(btreeSymbol) != SymbolType)
            || (nl->SymbolValue(btreeSymbol) != BTree::BasicType())){
        return listutils::typeError("Third argument should be btree!");
    }

    // check the rtree
    if(! listutils::isRTreeDescription(fourth)){
        return listutils::typeError("Fourth argument should be rtree!");
    }
    ListExpr rtreeKeyType = listutils::getRTreeType(fourth);
    if((! listutils::isSpatialType(rtreeKeyType)) && (! listutils::isRectangle(rtreeKeyType))){
        return listutils::typeError("Rtree not over a spatial attribute!");
    }
    ListExpr rtreeTupleDesc = nl->Second(fourth);
    if(! nl->Equal(rtreeTupleDesc, nl->Second(second))){
        return listutils::typeError("Type of rtree and  relation are different!");
    }
    // get the index of Id, UTrip and Trip
    // get the index of Id in stream
    attrname = nl->SymbolValue(fifth);
    attrtype = nl->Empty();
    attrlist = nl->Second(nl->Second(first));
    i1 = FindAttribute(attrlist, attrname, attrtype);
    if(i1 == 0){
        return listutils::typeError("Attribute name '"+attrname+"' is not know!");
    }else{
        if(nl->SymbolValue(attrtype) != CcInt::BasicType()){
            return listutils::typeError("Attribute type is not of type int!");
        }
    }
    // get the index of UTrip in stream
    attrname = nl->SymbolValue(sixth);
    attrtype = nl->Empty();
    i2 = FindAttribute(attrlist, attrname, attrtype);
    if(i2 == 0){
        return listutils::typeError("Attribute name '"+attrname+"' is not know!");
    }else{
        if(nl->SymbolValue(attrtype) != UPoint::BasicType()){
            return listutils::typeError("Attribute type is not of type upoint!");
        }
    }
    // get the index of Id in relation
    attrname = nl->SymbolValue(fifth);
    attrtype = nl->Empty();
    attrlist = nl->Second(nl->Second(second));
    i3 = FindAttribute(attrlist, attrname, attrtype);
    if(i3 == 0){
        return listutils::typeError("Attribute name '"+attrname+"' is not know!");
    }else{
        if(nl->SymbolValue(attrtype) != CcInt::BasicType()){
            return listutils::typeError("Attribute type is not of type int!");
        }
    }
    // get the index of Trip in relation
    attrname = nl->SymbolValue(seventh);
    attrtype = nl->Empty();
    i4 = FindAttribute(attrlist, attrname, attrtype);
    if(i4 == 0){
        return listutils::typeError("Attribute name '"+attrname+"' is not know!");
    }else{
        if(nl->SymbolValue(attrtype) != MPoint::BasicType()){
            return listutils::typeError("Attribute type is not of type mpoint!");
        }
    }
    // construct a result type, combine the result and attribute index    
    ListExpr resulttype = nl->SymbolAtom("bool");

    return nl->ThreeElemList(
            nl->SymbolAtom(Symbol::APPEND()),
            nl->FourElemList(
                nl->IntAtom(i1),
                nl->IntAtom(i2),
                nl->IntAtom(i3),
                nl->IntAtom(i4)),
            resulttype);
}


//
// get tuple id
//
TupleId getTupleId(CcInt *id, BTree *btree)
{
    BTreeIterator *it;
    //TupleId tid;
    assert(id->IsDefined());
    assert(btree);

    it = btree->ExactMatch(id);

    if(it == 0){
        return -1;
    }
    return it->GetId();
}

//value map function
//
// stream(tuple(Id int, UTrip upoint)) x rel(tuple(Id int, Trip mpoint)
// x btree x rtree x Id x UTrip x Trip
// -> bool
int UpdateTrajectoryVM(Word* args, Word& result, int message, Word& local, Supplier s)
{
    Relation *rel;
    R_Tree<3, TupleId> *rtree;
    BTree    *btree;
    Word     elem;
    Tuple    *tuple_old, *tuple_new, *tuple_stream, *tuple_rel;
    int      iid1, iutrip, iid2, itrip;
    CcInt    *vid;
    TupleId  tid;
    UPoint   *up;
    MPoint   *mp;
    vector<int> *changedIndeices = NULL;
    vector<Attribute *> *newAttrs = NULL;

    // get the local addr

    rel = (Relation *)args[1].addr;
    btree = (BTree *)args[2].addr;
    rtree = (R_Tree<3, TupleId> *)args[3].addr;
    iid1 = ((CcInt *)args[7].addr)->GetIntval() - 1;
    iutrip = ((CcInt *)args[8].addr)->GetIntval() - 1;
    iid2 = ((CcInt *)args[9].addr)->GetIntval() - 1;
    itrip = ((CcInt *)args[10].addr)->GetIntval() - 1;

    changedIndeices = new vector<int>(1);
    newAttrs = new vector<Attribute *>(1);
    //
    qp->Open(args[0].addr);
    qp->Request(args[0].addr, elem);
    while(qp->Received(args[0].addr)){
        tuple_stream = (Tuple *)elem.addr;
        vid = (CcInt *)tuple_stream->GetAttribute(iid1);
        up = ((UPoint *)tuple_stream->GetAttribute(iutrip))->Clone();

        // get the tupleid of object in relation, using btree
        tid = getTupleId(vid, btree);   
        // get the tuple storing the trajectory of object
        tuple_rel = rel->GetTuple(tid, true);
        mp = (MPoint *)tuple_rel->GetAttribute(itrip)->Clone();
        // update the trajectory
        mp->StartBulkLoad();
        mp->MergeAdd(*up);
        mp->EndBulkLoad();
        
        // save the change to relation
        (*changedIndeices)[0] = itrip;
        (*newAttrs)[0] = mp;
        rel->UpdateTuple(tuple_rel, *changedIndeices, *newAttrs);

        // update the rtree
        // very important
        
        // get next tuple in stream
        qp->Request(args[0].addr, elem);
    }
    qp->Close(args[0].addr);
    delete changedIndeices;
    delete newAttrs;
    result.addr = new CcBool(true);
    return 0;
}

//
// operator info
struct UpdateTrajectoryInfo : OperatorInfo {
    UpdateTrajectoryInfo()
    {
        name      = "updatetrajectory";
        signature = "((stream (tuple([Id:int, UTrip:upoint])))"
            "(rel(tuple(Id:int, Trip:mpoint))) x btree x rtree x Id x UTrip x Trip)"
            " -> stream (tuple (Upload uploadunit))";
        syntax    = "_ updatetrajectory [ _, _, _, _, _, _]";
        meaning   = "add a unit trajectory to trajectories";
    }
};

/****************************************************************************
 
   TrajUpdateAlgebra
 
 ***************************************************************************/

class TrajUpdateAlgebra : public Algebra
{
public:
    TrajUpdateAlgebra() : Algebra()
    {
        AddOperator( ConvertMP2UU2Info(), ConvertMP2UU2VM, ConvertMP2UU2TM );
        AddOperator( UpdateTrajectoryInfo(), UpdateTrajectoryVM, UpdateTrajectoryTM );
    }
};


/******************************************************************************

  Initialization of SETIAlgebra

 ******************************************************************************/

extern "C"
    Algebra*
InitializeTrajUpdateAlgebra( NestedList *nlRef,
        QueryProcessor *qpRef)
{
    nl = nlRef;
    qp = qpRef;
    return (new TrajUpdateAlgebra());
}
