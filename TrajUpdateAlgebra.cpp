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
    ListExpr tupleList = nl->OneElemList(
            nl->TwoElemList(nl->SymbolAtom("Upload"),
                nl->SymbolAtom(UploadUnit::BasicType())));
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
    };

    Iterator* iterator = static_cast<Iterator*>(local.addr);

    switch( message )
    {
        case OPEN:
            {
                iterator = new Iterator();
                local.addr = iterator;
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
                        TupleType* tupType =new TupleType(nl->Second(GetTupleResultType(s)));
                        Tuple* tup = new Tuple( tupType );
                        tup->PutAttribute( 0, ( Attribute* ) uu );
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
                        TupleType* tupType =new TupleType(nl->Second(GetTupleResultType(s)));
                        Tuple* tup = new Tuple( tupType );
                        tup->PutAttribute( 0, ( Attribute* ) uu );
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


class TrajUpdateAlgebra : public Algebra
{
public:
    TrajUpdateAlgebra() : Algebra()
    {
        AddOperator( ConvertMP2UU2Info(), ConvertMP2UU2VM, ConvertMP2UU2TM );
    }
};



/******************************************************************************

  15 Initialization of SETIAlgebra

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
