package multithreading;

import java.io.File;

public class execute_all {
    public static void main(String[] args){
        System.out.println("Running sequential execution");
        sequential.main(args);
        System.out.println("Running sequential execution with fibonacci");
        sequential_fib.main(args);
        System.out.println("Running no-lock execution");
        nolock.main(args);
        System.out.println("Running no-lock execution with fibonacci");
        nolock_fib.main(args);
        System.out.println("Running coarse-lock execution");
        coarse_lock.main(args);
        System.out.println("Running coarse-lock execution with fibonacci");
        coarse_lock_fib.main(args);
        System.out.println("Running fine-lock execution");
        fine_lock.main(args);
        System.out.println("Running fine-lock execution with fibonacci");
        fine_lock_fib.main(args);
        System.out.println("Running no-sharing execution");
        no_sharing.main(args);
        System.out.println("Running no-sharing execution with fibonacci");
        no_sharing_fib.main(args);

    }
}
