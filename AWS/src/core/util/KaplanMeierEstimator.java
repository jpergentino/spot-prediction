package core.util;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cloud.aws.dao.SpotDAO;
import core.executor.cbr.Case;

/*
 * Copyright (c) 2007-2011 by The Broad Institute of MIT and Harvard.  All Rights Reserved.
 *
 * This software is licensed under the terms of the GNU Lesser General Public License (LGPL),
 * Version 2.1 which is available at http://www.opensource.org/licenses/lgpl-2.1.php.
 *
 * THE SOFTWARE IS PROVIDED "AS IS." THE BROAD AND MIT MAKE NO REPRESENTATIONS OR
 * WARRANTES OF ANY KIND CONCERNING THE SOFTWARE, EXPRESS OR IMPLIED, INCLUDING,
 * WITHOUT LIMITATION, WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE, NONINFRINGEMENT, OR THE ABSENCE OF LATENT OR OTHER DEFECTS, WHETHER
 * OR NOT DISCOVERABLE.  IN NO EVENT SHALL THE BROAD OR MIT, OR THEIR RESPECTIVE
 * TRUSTEES, DIRECTORS, OFFICERS, EMPLOYEES, AND AFFILIATES BE LIABLE FOR ANY DAMAGES
 * OF ANY KIND, INCLUDING, WITHOUT LIMITATION, INCIDENTAL OR CONSEQUENTIAL DAMAGES,
 * ECONOMIC DAMAGES OR INJURY TO PROPERTY AND LOST PROFITS, REGARDLESS OF WHETHER
 * THE BROAD OR MIT SHALL BE ADVISED, SHALL HAVE OTHER REASON TO KNOW, OR IN FACT
 * SHALL KNOW OF THE POSSIBILITY OF THE FOREGOING.
 */

/**
 * Computes the Kaplan-Meier survival curve
 * Reference: http://cancerguide.org/scurve_km.html
 *
 * @author Pergentino
 */
public class KaplanMeierEstimator {

    /**
     * Return the kaplan-meier curve as a list of intervals.
     *
     * @param time     times in ascending order
     * @param censured array of boolean values indicating if the event is a failure or censure
     * @return
     */
    public static List<Interval> compute(int[] time, boolean[] censured) {

        if (time.length != censured.length) {
        	throw new RuntimeException("Time and censured sizes are different.");
        }
        if (time.length < 2) {
        	throw new RuntimeException("Time lenght is too small.");
        }


        // step 1 -- find the intervals
        List<Interval> intervals = new LinkedList<>();
        int startTime = 0;
        int endTime = 0;
        for (int i = 0; i < time.length; i++) {
            endTime = time[i];
            if (censured[i] == false && endTime > startTime) {
                intervals.add(new Interval(startTime, endTime));
                startTime = endTime;
            }
        }
        if (endTime > startTime) {
            intervals.add(new Interval(startTime, endTime));
        }

        // init variables.  Initially everyone is at risk, and the cumulative survival is 1
        float atRisk = time.length;
        float cumulativeSurvival = 1;
        Iterator<Interval> intervalIter = intervals.iterator();
        Interval currentInterval = intervalIter.next();
        currentInterval.setCumulativeSurvival(cumulativeSurvival);

        for (int i = 0; i < time.length; i++) {

            int t = time[i];

            // If we have moved past the current interval compute the cumulative survival and adjust the # at risk
            // for the start of the next interval.
            if (t > currentInterval.getEnd()) {
                atRisk -= currentInterval.getNumberCensured();
                float survivors = atRisk - currentInterval.getNumberDied();
                float tmp = survivors / atRisk;
                cumulativeSurvival *= tmp;

                // Skip to the next interval
                atRisk -= currentInterval.getNumberDied();
                while (intervalIter.hasNext() && t > currentInterval.getEnd()) {
                    currentInterval = intervalIter.next();
                    currentInterval.setCumulativeSurvival(cumulativeSurvival);
                }
            }

            if (censured[i]) {
                currentInterval.addCensure(time[i]);

            } else {
                currentInterval.incDied();
            }
        }
        currentInterval.setCumulativeSurvival(cumulativeSurvival);

        return intervals;

    }
    
    class IntArrayList {


        private transient int[] elements;

        private int size;


        public IntArrayList() {
            this(100);
        }

        public IntArrayList(int initialCapacity) {
            if (initialCapacity < 0)
                throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
            this.elements = new int[initialCapacity];
        }

        public IntArrayList(int[] elements) {
            this.elements = elements;
            size = elements.length;
        }

        public void add(int e) {
            if (size + 1 >= elements.length) {
                grow();
            }
            elements[size++] = e;
        }

        public void addAll(int[] args) {
            int[] newElements = new int[size + args.length];
            System.arraycopy(elements, 0, newElements, 0, size);
            System.arraycopy(args, 0, newElements, size, args.length);
            elements = newElements;
            size += args.length;
        }

        public void addAll(IntArrayList aList) {
            addAll(aList.toArray());
        }


        public int get(int idx) {
            return elements[idx];
        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return size == 0;
        }

        /**
         * Empty all elements.
         * This logically clears the collection but does not free up any space.
         */
        public void clear() {
            size = 0;
        }

        private void grow() {
            int oldCapacity = elements.length;
            int newCapacity;
            if (oldCapacity < 10000000) {
                newCapacity = oldCapacity * 2;
            } else {
                newCapacity = (oldCapacity * 3) / 2 + 1;
            }
            int[] tmp = new int[newCapacity];
            System.arraycopy(elements, 0, tmp, 0, elements.length);
            elements = tmp;
        }


        public int[] toArray() {
            trimToSize();
            return elements;
        }


        private void trimToSize() {
            int oldCapacity = elements.length;
            if (size < oldCapacity) {
                int[] tmp = new int[size];
                System.arraycopy(elements, 0, tmp, 0, size);
                elements = tmp;
            }
        }

        public void set(int idx, int i) {
            while(idx >= elements.length) {
            	grow();
            }
            elements[idx] = i;
            idx++;
            if (idx > size) { 
            	size = idx;  // Tried Math.max here, it showed up in cpu profiles!
            }
        }
    }


    public static class Interval {
    	
        private int start;
        private int end;
        private int numberDied;
        private IntArrayList censored = new KaplanMeierEstimator(). new IntArrayList();
        private float cumulativeSurvival;


        public Interval(int start, int end) {
            this.setStart(start);
            this.setEnd(end);
        }

        void incDied() {
            numberDied++;
        }

        void addCensure(int time) {
            censored.add(time);
        }

        public int getStart() {
            return start;
        }

        public void setStart(int start) {
            this.start = start;
        }

        public int getEnd() {
            return end;
        }

        public void setEnd(int end) {
            this.end = end;
        }

        public int getNumberDied() {
            return numberDied;
        }


        public IntArrayList getCensored() {
            return censored;
        }

        public float getCumulativeSurvival() {
            return cumulativeSurvival;
        }

        public void setCumulativeSurvival(float cumulativeSurvival) {
            this.cumulativeSurvival = cumulativeSurvival;
        }

        public int getNumberCensured() {
            return censored.size();
        }
    }
    
    /**
     * Return the kaplan-meier curve as a list of intervals.
     *
     * @param caseList list of {@link Case} with timeToRevocation and censored attributes.
     * @return the kaplan-meier curve as a list of intervals.
     */
    public static List<Interval> compute(List<Case> caseList) {
    	
        int[] cases = new int[caseList.size()]; 
        boolean[] censored = new boolean[caseList.size()];

        int cont = 0;
        for (Case obj : caseList) {
        	cases[cont] = obj.getTimeToRevocation();
        	censored[cont] = obj.isCensored();
        	cont++;
		}
        
    	return compute(cases, censored);
    }

    public static void main(String[] args) throws SQLException {

//        int[] survival = {1, 2, 3, 4, 5, 10, 120};
//        boolean[] alive = {false, false, false, false, false, false, true};
        
        SpotDAO dao = new SpotDAO();
//        List<Case> casesMap = dao.findCases("m4.large", 3, 20);
        List<Case> casesMap = dao.findCases("m3.medium", 3, 10);
        
        
        List<Interval> intervals = compute(casesMap);
        for (Interval i : intervals) {
//            System.out.println(i.getStart() + "\t" + i.getEnd() + "\t" + i.getNumberDied() + "\t" + i.getCensored().size() + "\t" + i.getCumulativeSurvival());
//            System.out.println(i.getEnd());
        	if (i.getCumulativeSurvival() > 0.90)
        		System.out.println(i.getEnd() +"\t"+ i.getNumberDied() +"\t"+ i.getNumberCensured() +"\t"+ i.getCumulativeSurvival());
        }

    }

}