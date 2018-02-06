package com.kz.test;

public class Decrease {  

    public static void main(String[] args) {  
        int A[] = { 9, 4, 3, 2, 5,10};  
        System.out.println("最长子序列为：");  
        max_dec_subseq(A, 6);  
    }  
  
    /* 
     * 遍历最长递减子序列，递归法 A - 源序列数组  
     * B - 通过DP求出的辅助数组  
     * k - 使得B[i]最大的i值 
     */  
    static void max_dec_subseq_traverse(int[] A, int[] B, int k) {  
    	//System.out.println(B[5]);
        for (int i = k; i >= 0; i--) {  
            if (A[i] > A[k] && B[k] == B[i] + 1) {  
                max_dec_subseq_traverse(A, B, i);  
                break;  
            }  
        }  
        System.out.println("A[" + k + "]=" + A[k]);  
    }  
  
    /* 
     * DP(动态规划)法求解最长递减子序列  
     * A - 源序列数组  
     * len - 数组大小 
     */  
    static void max_dec_subseq(int[] A, int len) {  
        int i, j, max_i = 0;  
        int[] B = new int[len];  
        for (i = 0; i < len; i++) {  
           // B[i] = 1;  
            for (j = 0; j < i; j++) {  
                if (A[j] > A[i] && (B[j] + 1) > B[i]) {  
                    B[i] = B[j] + 1;  
                    if (B[i] > B[max_i])  
                        max_i = i;  
                }  
            }  
        }  
        max_dec_subseq_traverse(A, B, max_i);  
    }  
  
}  
